package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/rs/zerolog/log"
)

// CreateWorkspace inserts a new workspace in the database.
// It automatically assigns a unique workspace ID if one is not provided.
// Returns an error if the label already exists, the label format is invalid,
// the catalog or variant ID is invalid, or there is a database error.
func (h *hatchCatalogDb) CreateWorkspace(ctx context.Context, workspace *models.Workspace) error {
	// Retrieve tenantID from the context
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	workspace.TenantID = tenantID

	// Generate a new UUID for the workspace ID if not already set
	if workspace.WorkspaceID == uuid.Nil {
		workspace.WorkspaceID = uuid.New()
	}

	// Set label as sql.NullString
	label := sql.NullString{String: workspace.Label, Valid: workspace.Label != ""}

	query := `
		INSERT INTO workspaces (workspace_id, label, description, info, base_version, variant_id, catalog_id, tenant_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING workspace_id;
	`

	// Execute the query and scan the returned workspace_id
	row := h.conn().QueryRowContext(ctx, query, workspace.WorkspaceID, label, workspace.Description, workspace.Info, workspace.BaseVersion, workspace.VariantID, workspace.CatalogID, workspace.TenantID)
	var insertedWorkspaceID uuid.UUID
	err := row.Scan(&insertedWorkspaceID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("label", workspace.Label).Str("variant_id", workspace.VariantID.String()).Str("catalog_id", workspace.CatalogID.String()).Msg("workspace already exists")
			return dberror.ErrAlreadyExists.Msg("workspace already exists")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "workspaces_label_variant_id_catalog_id_tenant_id_key" { // Unique constraint violation
				log.Ctx(ctx).Error().Str("label", workspace.Label).Str("variant_id", workspace.VariantID.String()).Str("catalog_id", workspace.CatalogID.String()).Msg("label already exists for the given variant and catalog")
				return dberror.ErrAlreadyExists.Msg("label already exists for the given variant and catalog")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "workspaces_label_check" { // Check constraint violation
				log.Ctx(ctx).Error().Str("label", workspace.Label).Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")
			}
			if pgErr.ConstraintName == "workspaces_variant_id_catalog_id_tenant_id_fkey" { // Foreign key constraint violation
				log.Ctx(ctx).Info().Str("variant_id", workspace.VariantID.String()).Str("catalog_id", workspace.CatalogID.String()).Msg("catalog or variant not found")
				return dberror.ErrInvalidCatalog
			}
		}
		log.Ctx(ctx).Error().Err(err).Str("label", workspace.Label).Str("variant_id", workspace.VariantID.String()).Str("catalog_id", workspace.CatalogID.String()).Msg("failed to insert workspace")
		return dberror.ErrDatabase.Err(err)
	}

	workspace.WorkspaceID = insertedWorkspaceID
	return nil
}

// DeleteWorkspace deletes a workspace from the database based on workspace_id and tenant_id.
// Returns an error if there is a database error.
func (h *hatchCatalogDb) DeleteWorkspace(ctx context.Context, workspaceID uuid.UUID) error {
	// Retrieve tenantID from the context
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		DELETE FROM workspaces
		WHERE workspace_id = $1 AND tenant_id = $2;
	`

	result, err := h.conn().ExecContext(ctx, query, workspaceID, tenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete workspace")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("workspace_id", workspaceID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
	}

	return nil
}

func (h *hatchCatalogDb) GetWorkspace(ctx context.Context, workspaceID uuid.UUID) (*models.Workspace, error) {
	// Retrieve tenantID from the context
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT workspace_id, label, description, info, base_version, parameters_directory, collections_directory, variant_id, catalog_id, tenant_id, created_at, updated_at
		FROM workspaces
		WHERE workspace_id = $1 AND tenant_id = $2;
	`

	row := h.conn().QueryRowContext(ctx, query, workspaceID, tenantID)
	workspace := &models.Workspace{}
	err := row.Scan(
		&workspace.WorkspaceID, &workspace.Label, &workspace.Description, &workspace.Info,
		&workspace.BaseVersion, &workspace.ParametersDir, &workspace.CollectionsDir, &workspace.VariantID, &workspace.CatalogID, &workspace.TenantID,
		&workspace.CreatedAt, &workspace.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("workspace_id", workspaceID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
			return nil, dberror.ErrNotFound.Msg("workspace not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve workspace")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return workspace, nil
}

// UpdateWorkspaceLabel updates the label of a workspace based on its workspace_id and tenant_id.
// Returns an error if the new label already exists, the label format is invalid, or there is a database error.
func (h *hatchCatalogDb) UpdateWorkspaceLabel(ctx context.Context, workspaceID uuid.UUID, newLabel string) error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if newLabel == "" {
		log.Ctx(ctx).Error().Msg("new label cannot be empty")
		return dberror.ErrInvalidInput.Msg("label cannot be empty")
	}

	query := `
		UPDATE workspaces
		SET label = $1, updated_at = NOW()
		WHERE workspace_id = $2 AND tenant_id = $3
		RETURNING workspace_id;
	`

	row := h.conn().QueryRowContext(ctx, query, newLabel, workspaceID, tenantID)
	var returnedWorkspaceID uuid.UUID
	err := row.Scan(&returnedWorkspaceID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("workspace_id", workspaceID.String()).Str("tenant_id", string(tenantID)).Msg("workspace not found")
			return dberror.ErrNotFound.Msg("workspace not found")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "workspaces_label_variant_id_catalog_id_tenant_id_key" { // Unique constraint violation
				log.Ctx(ctx).Error().Str("label", newLabel).Str("workspace_id", workspaceID.String()).Msg("label already exists for another workspace")
				return dberror.ErrAlreadyExists.Msg("label already exists for another workspace")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "workspaces_label_check" { // Check constraint violation
				log.Ctx(ctx).Error().Str("label", newLabel).Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")
			}
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update workspace label")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}
