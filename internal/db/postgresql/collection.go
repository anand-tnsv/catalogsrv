package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/rs/zerolog/log"
)

func (h *hatchCatalogDb) CreateCollection(ctx context.Context, c *models.Collection, ref ...models.CollectionRef) (err apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	c.TenantID = tenantID

	tx, errStd := h.conn().BeginTx(ctx, nil)
	if errStd != nil {
		log.Ctx(ctx).Error().Err(errStd).Msg("failed to begin transaction")
		return dberror.ErrDatabase.Err(errStd)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Ctx(ctx).Error().Err(rollbackErr).Msg("failed to rollback transaction")
			}
		}
	}()

	err = h.createCollectionWithTransaction(ctx, c, tx, ref...)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to create collection")
		return err
	}

	if errStd := tx.Commit(); errStd != nil {
		log.Ctx(ctx).Error().Err(errStd).Msg("failed to commit transaction")
		return dberror.ErrDatabase.Err(errStd)
	}

	return nil
}

func (h *hatchCatalogDb) createCollectionWithTransaction(ctx context.Context, c *models.Collection, tx *sql.Tx, ref ...models.CollectionRef) apperrors.Error {
	description := sql.NullString{String: c.Description, Valid: c.Description != ""}

	collectionID := c.CollectionID
	if collectionID == uuid.Nil {
		collectionID = uuid.New()
	}

	var row *sql.Row
	if len(ref) > 0 && ref[0].IsValid() {
		query := `
			INSERT INTO collections (
				collection_id, path, hash, description, namespace, collection_schema,
				info, repo_id, variant_id, tenant_id
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8,
				(SELECT variant_id FROM variants
				WHERE name = $9
				AND catalog_id = (
					SELECT catalog_id FROM catalogs
					WHERE name = $10 AND tenant_id = $11
				)
				AND tenant_id = $11),
				$11
			)
			RETURNING collection_id;
		`

		row = tx.QueryRowContext(ctx, query,
			collectionID,       // $1
			c.Path,             // $2
			c.Hash,             // $3
			description,        // $4
			ref[0].Namespace,   // $5
			c.CollectionSchema, // $6
			c.Info,             // $7
			c.RepoID,           // $8
			ref[0].Variant,     // $9 - variant name
			ref[0].Catalog,     // $10 - catalog name
			c.TenantID,         // $11
		)
	} else {
		query := `
			INSERT INTO collections (
				collection_id, path, hash, description, namespace, collection_schema,
				info, repo_id, variant_id, tenant_id
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			RETURNING collection_id
		`

		row = tx.QueryRowContext(ctx, query,
			collectionID, c.Path, c.Hash, description, c.Namespace, c.CollectionSchema,
			c.Info, c.RepoID, c.VariantID, c.TenantID,
		)
	}
	var insertedID uuid.UUID
	err := row.Scan(&insertedID)
	if err != nil {
		if err == sql.ErrNoRows {
			return dberror.ErrAlreadyExists.Msg("collection already exists")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			switch {
			case pgErr.Code == "23505":
				return dberror.ErrAlreadyExists.Msg("collection already exists")
			case pgErr.Code == "23514" && pgErr.ConstraintName == "collections_path_check":
				return dberror.ErrInvalidInput.Msg("invalid path format")
			case pgErr.Code == "23514" && pgErr.ConstraintName == "collections_namespace_check":
				return dberror.ErrInvalidInput.Msg("invalid namespace format")
			case pgErr.Code == "23514" && pgErr.ConstraintName == "collections_collection_schema_check":
				return dberror.ErrInvalidInput.Msg("invalid collection_schema format")
			case pgErr.ConstraintName == "collections_variant_id_tenant_id_fkey":
				return dberror.ErrInvalidInput.Msg("referenced variant not found")
			}
		}
		return dberror.ErrDatabase.Err(err)
	}

	c.CollectionID = insertedID

	return nil
}

func (h *hatchCatalogDb) GetCollection(ctx context.Context, path, namespace string, repoID, variantID uuid.UUID) (*models.Collection, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT collection_id, path, hash, description, namespace, collection_schema, info,
		       repo_id, variant_id, tenant_id
		FROM collections
		WHERE path = $1 AND namespace = $2 AND repo_id = $3
		  AND variant_id = $4 AND tenant_id = $5
	`

	var c models.Collection
	err := h.conn().QueryRowContext(ctx, query, path, namespace, repoID, variantID, tenantID).
		Scan(&c.CollectionID, &c.Path, &c.Hash, &c.Description, &c.Namespace, &c.CollectionSchema,
			&c.Info, &c.RepoID, &c.VariantID, &c.TenantID)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("collection not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &c, nil
}

func (h *hatchCatalogDb) UpdateCollection(ctx context.Context, c *models.Collection) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	description := sql.NullString{String: c.Description, Valid: c.Description != ""}

	query := `
		UPDATE collections
		SET hash = $1,
		    description = $2,
		    info = $3
		WHERE path = $4 AND namespace = $5 AND repo_id = $6
		  AND variant_id = $7 AND tenant_id = $8
	`

	result, err := h.conn().ExecContext(ctx, query,
		c.Hash, description, c.Info,
		c.Path, c.Namespace, c.RepoID, c.VariantID, tenantID,
	)

	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("collection not found")
	}

	return nil
}

func (h *hatchCatalogDb) DeleteCollection(ctx context.Context, path, namespace string, repoID, variantID uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		DELETE FROM collections
		WHERE path = $1 AND namespace = $2 AND repo_id = $3
		  AND variant_id = $4 AND tenant_id = $5
	`

	result, err := h.conn().ExecContext(ctx, query, path, namespace, repoID, variantID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("collection not found")
	}

	return nil
}

func (h *hatchCatalogDb) ListCollectionsByNamespace(ctx context.Context, namespace string, repoID, variantID uuid.UUID) ([]*models.Collection, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT collection_id, path, hash, description, namespace, collection_schema, info,
		       repo_id, variant_id, tenant_id
		FROM collections
		WHERE namespace = $1 AND repo_id = $2
		  AND variant_id = $3 AND tenant_id = $4
		ORDER BY path
	`

	rows, err := h.conn().QueryContext(ctx, query, namespace, repoID, variantID, tenantID)
	if err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	var collections []*models.Collection
	for rows.Next() {
		var c models.Collection
		err := rows.Scan(&c.CollectionID, &c.Path, &c.Hash, &c.Description, &c.Namespace,
			&c.CollectionSchema, &c.Info, &c.RepoID, &c.VariantID, &c.TenantID)
		if err != nil {
			return nil, dberror.ErrDatabase.Err(err)
		}
		collections = append(collections, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}

	return collections, nil
}
