package postgresql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"

	"github.com/rs/zerolog/log"
)

type tenantProject struct {
	c dbmanager.ScopedConn
}

func (tp *tenantProject) conn() *sql.Conn {
	return tp.c.Conn()
}

func NewTenantProjectManager(c dbmanager.ScopedConn) *tenantProject {
	return &tenantProject{c: c}
}

// CreateProjectAndTenant creates a new project and tenant in the database.
func (tp *tenantProject) CreateProjectAndTenant(ctx context.Context, projectID types.ProjectId, tenantID types.TenantId) error {
	// Create the tenant
	err := tp.CreateTenant(ctx, tenantID)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return nil
		}
		return err
	}
	// Create the project
	err = tp.CreateProject(ctx, projectID)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return nil
		}
		return err
	}
	return nil
}

// CreateTenant inserts a new tenant into the database.
// If the tenant already exists, it does nothing.
func (tp *tenantProject) CreateTenant(ctx context.Context, tenantID types.TenantId) error {
	query := `
		INSERT INTO tenants (tenant_id)
		VALUES ($1)
		ON CONFLICT (tenant_id) DO NOTHING
		RETURNING tenant_id;
	`

	// Execute the query directly using tp.conn().QueryRowContext
	row := tp.conn().QueryRowContext(ctx, query, string(tenantID))
	var insertedTenantID string
	err := row.Scan(&insertedTenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("tenant_id", string(tenantID)).Msg("tenant already exists")
			return dberror.ErrAlreadyExists.Msg("tenant already exists")
		}
		log.Ctx(ctx).Error().Str("tenant_id", string(tenantID)).Msg("failed to insert tenant")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

// GetTenant retrieves a tenant from the database.
func (tp *tenantProject) GetTenant(ctx context.Context, tenantID types.TenantId) (*models.Tenant, error) {
	query := `
		SELECT tenant_id
		FROM tenants
		WHERE tenant_id = $1;
	`

	// Execute the query directly using tp.conn().QueryRowContext
	row := tp.conn().QueryRowContext(ctx, query, string(tenantID))

	var tenant models.Tenant
	err := row.Scan(&tenant.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("tenant_id", string(tenantID)).Msg("tenant not found")
			return nil, dberror.ErrNotFound.Msg("tenant not found")
		}
		log.Ctx(ctx).Error().Str("tenant_id", string(tenantID)).Msg("failed to retrieve tenant")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &tenant, nil
}

// DeleteTenant deletes a tenant from the database.
func (tp *tenantProject) DeleteTenant(ctx context.Context, tenantID types.TenantId) error {
	query := `
		DELETE FROM tenants
		WHERE tenant_id = $1;
	`
	_, err := tp.conn().ExecContext(ctx, query, string(tenantID))
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Str("tenant_id", string(tenantID)).Msg("failed to delete tenant")
		return dberror.ErrDatabase.Err(err)
	}
	return nil
}

// CreateProject inserts a new project into the database.
func (tp *tenantProject) CreateProject(ctx context.Context, projectID types.ProjectId) error {
	tenantID := common.TenantIdFromContext(ctx)

	// Validate tenantID to ensure it is not empty
	if tenantID == "" {
		log.Ctx(ctx).Error().Msg("tenant ID is missing from context")
		return dberror.ErrInvalidInput.Msg("tenant ID is required")
	}

	query := `
		INSERT INTO projects (project_id, tenant_id)
		VALUES ($1, $2)
		ON CONFLICT (project_id, tenant_id) DO NOTHING
		RETURNING project_id;
	`

	// Execute the query directly using tp.conn().QueryRowContext
	row := tp.conn().QueryRowContext(ctx, query, string(projectID), string(tenantID))
	var insertedProjectID string
	err := row.Scan(&insertedProjectID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("project_id", string(projectID)).Msg("project already exists")
			return dberror.ErrAlreadyExists.Msg("project already exists")
		}
		log.Ctx(ctx).Error().Str("project_id", string(projectID)).Msg("failed to insert project")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

// GetProject retrieves a project from the database.
func (tp *tenantProject) GetProject(ctx context.Context, projectID types.ProjectId) (*models.Project, error) {
	tenantID := common.TenantIdFromContext(ctx)

	// Validate tenantID to ensure it is not empty
	if tenantID == "" {
		log.Ctx(ctx).Error().Msg("tenant ID is missing from context")
		return nil, dberror.ErrInvalidInput.Msg("tenant ID is required")
	}

	query := `
		SELECT project_id, tenant_id
		FROM projects
		WHERE project_id = $1 AND tenant_id = $2;
	`

	// Query the project data
	row := tp.conn().QueryRowContext(ctx, query, string(projectID), string(tenantID))

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

// DeleteProject deletes a project from the database. If the project does not exist, it does nothing.
func (tp *tenantProject) DeleteProject(ctx context.Context, projectID types.ProjectId) error {
	tenantID := common.TenantIdFromContext(ctx)

	// Validate tenantID to ensure it is not empty
	if tenantID == "" {
		log.Ctx(ctx).Error().Msg("tenant ID is missing from context")
		return dberror.ErrInvalidInput.Msg("tenant ID is required")
	}

	query := `
		DELETE FROM projects
		WHERE project_id = $1 AND tenant_id = $2;
	`
	_, err := tp.conn().ExecContext(ctx, query, string(projectID), string(tenantID))
	if err != nil {
		log.Ctx(ctx).Error().
			Err(err).
			Str("project_id", string(projectID)).
			Msg("failed to delete project")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}
