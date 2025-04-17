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

func (h *hatchCatalogDb) CreateWorkspaceCollection(ctx context.Context, wc *models.WorkspaceCollection) (err apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	wc.TenantID = tenantID

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

	err = h.createWorkspaceCollectionWithTransaction(ctx, wc, tx)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to create workspace collection")
		return err
	}

	if errStd := tx.Commit(); errStd != nil {
		log.Ctx(ctx).Error().Err(errStd).Msg("failed to commit transaction")
		return dberror.ErrDatabase.Err(errStd)
	}

	return nil
}

func (h *hatchCatalogDb) createWorkspaceCollectionWithTransaction(ctx context.Context, wc *models.WorkspaceCollection, tx *sql.Tx) apperrors.Error {
	description := sql.NullString{String: wc.Description, Valid: wc.Description != ""}

	workspaceCollectionID := wc.CollectionID
	if workspaceCollectionID == uuid.Nil {
		workspaceCollectionID = uuid.New()
	}

	query := `
		INSERT INTO workspace_collections (
			collection_id, path, hash, description, namespace, collection_schema,
			info, workspace_id, variant_id, catalog_id, tenant_id
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING collection_id
	`

	row := tx.QueryRowContext(ctx, query,
		workspaceCollectionID, wc.Path, wc.Hash, description, wc.Namespace, wc.CollectionSchema,
		wc.Info, wc.WorkspaceID, wc.VariantID, wc.CatalogID, wc.TenantID,
	)
	var insertedWorkspaceCollectionID uuid.UUID
	err := row.Scan(&insertedWorkspaceCollectionID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("workspace", wc.WorkspaceID.String()).Str("variant_id", wc.VariantID.String()).Str("catalog_id", wc.CatalogID.String()).Msg("workspace collection already exists")
			return dberror.ErrAlreadyExists.Msg("workspace already exists")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			switch {
			case pgErr.Code == "23505":
				return dberror.ErrAlreadyExists.Msg("workspace collection already exists")
			case pgErr.Code == "23514" && pgErr.ConstraintName == "workspace_collections_path_check":
				return dberror.ErrInvalidInput.Msg("invalid path format")
			case pgErr.Code == "23514" && pgErr.ConstraintName == "workspace_collections_namespace_check":
				return dberror.ErrInvalidInput.Msg("invalid namespace format")
			case pgErr.Code == "23514" && pgErr.ConstraintName == "workspace_collections_collection_schema_check":
				return dberror.ErrInvalidInput.Msg("invalid collection_schema format")
			case pgErr.ConstraintName == "workspace_collections_namespace_variant_id_catalog_id_tenant_id_fkey":
				return dberror.ErrInvalidInput.Msg("referenced namespace not found")
			case pgErr.ConstraintName == "workspace_collections_workspace_id_tenant_id_fkey":
				return dberror.ErrInvalidInput.Msg("referenced workspace not found")
			}
		}
		return dberror.ErrDatabase.Err(err)
	}

	wc.CollectionID = insertedWorkspaceCollectionID

	return nil
}

func (h *hatchCatalogDb) GetWorkspaceCollection(ctx context.Context, path, namespace string, workspaceID, variantID, catalogID uuid.UUID) (*models.WorkspaceCollection, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT collection_id, path, hash, description, namespace, collection_schema, info,
		       workspace_id, variant_id, catalog_id, tenant_id
		FROM workspace_collections
		WHERE path = $1 AND namespace = $2 AND workspace_id = $3
		  AND variant_id = $4 AND catalog_id = $5 AND tenant_id = $6
	`

	var wc models.WorkspaceCollection
	// fmt.printf all input parameters
	log.Ctx(ctx).Debug().
		Str("path", path).
		Str("namespace", namespace).
		Str("workspace_id", workspaceID.String()).
		Str("variant_id", variantID.String()).
		Str("catalog_id", catalogID.String()).
		Str("tenant_id", string(tenantID)).
		Msg("GetWorkspaceCollection parameters")

	err := h.conn().QueryRowContext(ctx, query, path, namespace, workspaceID, variantID, catalogID, tenantID).
		Scan(&wc.CollectionID, &wc.Path, &wc.Hash, &wc.Description, &wc.Namespace, &wc.CollectionSchema,
			&wc.Info, &wc.WorkspaceID, &wc.VariantID, &wc.CatalogID, &wc.TenantID)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("workspace collection not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &wc, nil
}

func (h *hatchCatalogDb) UpdateWorkspaceCollection(ctx context.Context, wc *models.WorkspaceCollection) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	description := sql.NullString{String: wc.Description, Valid: wc.Description != ""}

	query := `
		UPDATE workspace_collections
		SET hash = $1,
		    description = $2,
		    info = $3
		WHERE path = $4 AND namespace = $5 AND workspace_id = $6
		  AND variant_id = $7 AND catalog_id = $8 AND tenant_id = $9
	`

	result, err := h.conn().ExecContext(ctx, query,
		wc.Hash, description, wc.Info,
		wc.Path, wc.Namespace, wc.WorkspaceID, wc.VariantID, wc.CatalogID, tenantID,
	)

	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("workspace collection not found")
	}

	return nil
}

func (h *hatchCatalogDb) DeleteWorkspaceCollection(ctx context.Context, path, namespace string, workspaceID, variantID, catalogID uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		DELETE FROM workspace_collections
		WHERE path = $1 AND namespace = $2 AND workspace_id = $3
		  AND variant_id = $4 AND catalog_id = $5 AND tenant_id = $6
	`

	result, err := h.conn().ExecContext(ctx, query, path, namespace, workspaceID, variantID, catalogID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("workspace collection not found")
	}

	return nil
}

func (h *hatchCatalogDb) ListWorkspaceCollectionsByNamespace(ctx context.Context, namespace string, workspaceID, variantID, catalogID uuid.UUID) ([]*models.WorkspaceCollection, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT collection_id, path, hash, description, namespace, collection_schema, info,
		       workspace_id, variant_id, catalog_id, tenant_id
		FROM workspace_collections
		WHERE namespace = $1 AND workspace_id = $2
		  AND variant_id = $3 AND catalog_id = $4 AND tenant_id = $5
		ORDER BY path
	`

	rows, err := h.conn().QueryContext(ctx, query, namespace, workspaceID, variantID, catalogID, tenantID)
	if err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	var collections []*models.WorkspaceCollection
	for rows.Next() {
		var wc models.WorkspaceCollection
		err := rows.Scan(&wc.CollectionID, &wc.Path, &wc.Hash, &wc.Description, &wc.Namespace,
			&wc.CollectionSchema, &wc.Info, &wc.WorkspaceID, &wc.VariantID, &wc.CatalogID, &wc.TenantID)
		if err != nil {
			return nil, dberror.ErrDatabase.Err(err)
		}
		collections = append(collections, &wc)
	}
	if err := rows.Err(); err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}

	return collections, nil
}
