package postgresql

import (
	"context"
	"database/sql"

	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/internal/types"

	"github.com/rs/zerolog/log"
)

// CreateTenant inserts a new tenant into the database.
// If the tenant already exists, it does nothing.
func (h *hatchCatalogDb) CreateTenant(ctx context.Context, tenantID types.TenantId) error {
	query := `
		INSERT INTO tenants (tenant_id)
		VALUES ($1)
		ON CONFLICT (tenant_id) DO NOTHING;
	`

	// Execute the query directly using h.conn().ExecContext
	result, err := h.conn().ExecContext(ctx, query, string(tenantID))
	if err != nil {
		log.Ctx(ctx).Info().Str("tenant_id", string(tenantID)).Msgf("failed to insert tenant")
		return dberror.ErrDatabase.Err(err)
	}
	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Info().Str("tenant_id", string(tenantID)).Msgf("failed to retrieve rows affected")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("tenant_id", string(tenantID)).Msgf("tenant already exists")
		return dberror.ErrAlreadyExists.Msg("tenant already exists")
	}

	return nil
}

// GetTenant retrieves a tenant from the database.
func (h *hatchCatalogDb) GetTenant(ctx context.Context, tenantID types.TenantId) (*models.Tenant, error) {
	query := `
		SELECT tenant_id
		FROM tenants
		WHERE tenant_id = $1;
	`

	// Execute the query directly using h.conn().QueryRowContext
	row := h.conn().QueryRowContext(ctx, query, string(tenantID))

	var tenant models.Tenant
	err := row.Scan(&tenant.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("tenant_id", string(tenantID)).Msg("tenant not found")
			return nil, dberror.ErrNotFound.Msg("tenant not found")
		}
		log.Ctx(ctx).Info().Str("tenant_id", string(tenantID)).Msgf("failed to retrieve tenant")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &tenant, nil
}

// DeleteTenant deletes a tenant from the database.
func (h *hatchCatalogDb) DeleteTenant(ctx context.Context, tenantID types.TenantId) error {
	query := `
		DELETE FROM tenants
		WHERE tenant_id = $1;
	`
	_, err := h.conn().ExecContext(ctx, query, string(tenantID))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("tenant_id", string(tenantID)).Msg("failed to delete tenant")
		return dberror.ErrDatabase.Err(err)
	}
	return nil
}

// CreateProject inserts a new project into the database.
func (h *hatchCatalogDb) CreateProject(ctx context.Context, projectID types.ProjectId) error {
	tenantID := common.TenantIdFromContext(ctx)

	query := `
		INSERT INTO projects (project_id, tenant_id)
		VALUES ($1, $2)
		ON CONFLICT (project_id, tenant_id) DO NOTHING;
	`

	// Execute the query directly using h.conn().ExecContext
	result, err := h.conn().ExecContext(ctx, query, string(projectID), string(tenantID))
	if err != nil {
		log.Ctx(ctx).Info().Str("project_id", string(projectID)).Msg("failed to insert project")
		return dberror.ErrDatabase.Err(err)
	}

	// Check if any rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Info().Str("project_id", string(projectID)).Msg("failed to retrieve rows affected")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("project_id", string(projectID)).Msg("project already exists")
		return dberror.ErrAlreadyExists.Msg("project already exists")
	}

	return nil
}

// GetProject retrieves a project from the database.
func (h *hatchCatalogDb) GetProject(ctx context.Context, projectID types.ProjectId) (*models.Project, error) {
	tenantID := common.TenantIdFromContext(ctx)
	query := `
		SELECT project_id, tenant_id
		FROM projects
		WHERE project_id = $1 AND tenant_id = $2;
	`

	// Query the project data
	row := h.conn().QueryRowContext(ctx, query, string(projectID), string(tenantID))

	var project models.Project
	err := row.Scan(&project.ProjectID, &project.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().
				Str("project_id", string(projectID)).
				Msg("project not found")
			return nil, dberror.ErrNotFound.Msg("project not found")
		}
		log.Ctx(ctx).Error().
			Str("project_id", string(projectID)).
			Msg("failed to retrieve project")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &project, nil
}

// DeleteProject deletes a project from the database.
func (h *hatchCatalogDb) DeleteProject(ctx context.Context, projectID types.ProjectId) error {
	tenantID := common.TenantIdFromContext(ctx)
	query := `
		DELETE FROM projects
		WHERE project_id = $1 AND tenant_id = $2;
	`

	// Execute the delete operation
	_, err := h.conn().ExecContext(ctx, query, string(projectID), string(tenantID))
	if err != nil {
		log.Ctx(ctx).Error().
			Str("project_id", string(projectID)).
			Msg("failed to delete project")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}
