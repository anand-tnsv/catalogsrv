package postgresql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/config"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

func (h *hatchCatalogDb) UpsertCollection(ctx context.Context, c *models.Collection, ref ...models.CollectionRef) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	c.TenantID = tenantID

	description := sql.NullString{String: c.Description, Valid: c.Description != ""}

	// attempt upsert up to 2 times (original + one retry on pkey conflict)
	for attempt := 0; attempt < 2; attempt++ {
		if c.CollectionID == uuid.Nil {
			c.CollectionID = uuid.New()
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
				ON CONFLICT (path, repo_id, variant_id, tenant_id) DO UPDATE
				SET hash = EXCLUDED.hash,
					description = EXCLUDED.description,
					info = EXCLUDED.info
				RETURNING collection_id;
			`

			row = h.conn().QueryRowContext(ctx, query,
				c.CollectionID,     // $1
				c.Path,             // $2
				c.Hash,             // $3
				description,        // $4
				ref[0].Namespace,   // $5
				c.CollectionSchema, // $6
				c.Info,             // $7
				c.RepoID,           // $8
				ref[0].Variant,     // $9
				ref[0].Catalog,     // $10
				c.TenantID,         // $11
			)
		} else {
			query := `
				INSERT INTO collections (
					collection_id, path, hash, description, namespace, collection_schema,
					info, repo_id, variant_id, tenant_id
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (path, repo_id, variant_id, tenant_id) DO UPDATE
				SET hash = EXCLUDED.hash,
					description = EXCLUDED.description,
					info = EXCLUDED.info
				RETURNING collection_id;
			`

			row = h.conn().QueryRowContext(ctx, query,
				c.CollectionID, c.Path, c.Hash, description, c.Namespace, c.CollectionSchema,
				c.Info, c.RepoID, c.VariantID, c.TenantID,
			)
		}

		var insertedID uuid.UUID
		err := row.Scan(&insertedID)
		if err == nil {
			c.CollectionID = insertedID
			return nil
		}

		// handle error
		if pgErr, ok := err.(*pgconn.PgError); ok {
			switch {
			case pgErr.ConstraintName == "collections_pkey" && attempt == 0:
				log.Ctx(ctx).Info().Msg("collection_id conflict, generating new ID and retrying")
				c.CollectionID = uuid.Nil // will regenerate next loop
				continue
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

	return dberror.ErrDatabase.Msg("collection insert retry exhausted")
}

func (h *hatchCatalogDb) GetCollection(ctx context.Context, path, namespace string, repoID, variantID uuid.UUID) (*models.Collection, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	var description sql.NullString
	query := `
		SELECT collection_id, path, hash, description, namespace, collection_schema, info,
			repo_id, variant_id, tenant_id
		FROM (
			SELECT collection_id, path, hash, description, namespace, collection_schema, info,
				repo_id, variant_id, tenant_id
			FROM collections
			WHERE path = $1 AND repo_id = $2
			AND variant_id = $3 AND tenant_id = $4 AND namespace != '--deleted--'
			UNION ALL
			SELECT collection_id, path, hash, description, namespace, collection_schema, info,
				repo_id, variant_id, tenant_id
			FROM collections
			WHERE path = $1 AND repo_id = $3
			AND variant_id = $3 AND tenant_id = $4 AND namespace != '--deleted--'
		) AS fallback
		LIMIT 1;
	`

	var c models.Collection
	err := h.conn().QueryRowContext(ctx, query, path, repoID, variantID, tenantID).
		Scan(&c.CollectionID, &c.Path, &c.Hash, &description, &c.Namespace, &c.CollectionSchema,
			&c.Info, &c.RepoID, &c.VariantID, &c.TenantID)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("collection not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}
	if description.Valid {
		c.Description = description.String
	}

	return &c, nil
}

func (h *hatchCatalogDb) GetCollectionObject(ctx context.Context, path, namespace string, repoID, variantID uuid.UUID) (*models.CatalogObject, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	log.Ctx(ctx).Debug().Msgf("GetCollectionObject: path=%s, namespace=%s, repoID=%s, variantID=%s, tenantID=%s", path, namespace, repoID.String(), variantID.String(), tenantID)
	// If the collection is not found in the specified repo, try to find it in the active repo which is the variant_id
	query := `
		SELECT hash, type, version, tenant_id, data
		FROM catalog_objects
		WHERE hash = (
			SELECT hash FROM (
				SELECT hash FROM collections
				WHERE path = $1 AND repo_id = $2
				AND variant_id = $3 AND tenant_id = $4
				AND namespace != '--deleted--'
				UNION ALL
				SELECT hash FROM collections
				WHERE path = $1 AND repo_id = $3
				AND variant_id = $3 AND tenant_id = $4
				AND namespace != '--deleted--'
			) AS fallback
			LIMIT 1
		)
		AND tenant_id = $4;
	`
	var hash, version string
	var objType types.CatalogObjectType
	var data []byte
	err := h.conn().QueryRowContext(ctx, query, path, repoID, variantID, tenantID).
		Scan(&hash, &objType, &version, &tenantID, &data)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("collection not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	// Create and populate the CatalogObject
	catalogObj := &models.CatalogObject{
		Hash:     hash,
		Type:     objType,
		Version:  version,
		TenantID: tenantID,
	}

	catalogObj.Data = data
	// Decompress the data
	if config.CompressCatalogObjects {
		catalogObj.Data, err = snappy.Decode(nil, data)
		if err != nil {
			return nil, dberror.ErrDatabase.Err(err)
		}
	}

	return catalogObj, nil
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
		WHERE path = $4
		AND repo_id = $5
		AND variant_id = $6
		AND tenant_id = $7
		AND namespace != '--deleted--';
	`

	result, err := h.conn().ExecContext(ctx, query,
		c.Hash, description, c.Info,
		c.Path, c.RepoID, c.VariantID, tenantID,
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

func (h *hatchCatalogDb) DeleteCollection(ctx context.Context, path, namespace string, repoID, variantID uuid.UUID) (string, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", dberror.ErrMissingTenantID
	}

	isWorkspace := (repoID != variantID)

	var query string
	if isWorkspace {
		query = `
			WITH deleted AS (
				UPDATE collections SET namespace = '--deleted--'
				WHERE path = $1 AND repo_id = $2
				  AND variant_id = $3 AND tenant_id = $4
				RETURNING hash
			)
			SELECT hash FROM deleted;
		`
	} else {
		query = `
			WITH deleted AS (
				DELETE FROM collections
				WHERE path = $1 AND repo_id = $2
				  AND variant_id = $3 AND tenant_id = $4
				RETURNING hash
			)
			SELECT hash FROM deleted;
		`
	}

	var deletedHash string
	err := h.conn().QueryRowContext(ctx, query, path, repoID, variantID, tenantID).Scan(&deletedHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", dberror.ErrNotFound.Msg("collection not found")
		}
		return "", dberror.ErrDatabase.Err(err)
	}

	if isWorkspace {
		deletedHash = ""
	}

	return deletedHash, nil
}

func (h *hatchCatalogDb) HasReferencesToCollectionSchema(ctx context.Context, collectionSchema, namespace string, repoID, variantID uuid.UUID) (bool, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return false, dberror.ErrMissingTenantID
	}

	query := `
		SELECT EXISTS (
			SELECT 1 FROM collections
			WHERE collection_schema = $1
			  AND namespace = $2
			  AND repo_id = $3
			  AND variant_id = $4
			  AND tenant_id = $5
		);
	`
	var exists bool
	err := h.conn().QueryRowContext(ctx, query, collectionSchema, namespace, repoID, variantID, tenantID).Scan(&exists)
	if err != nil {
		return false, dberror.ErrDatabase.Err(err)
	}
	return exists, nil
}
