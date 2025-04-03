package postgresql

import (
	"context"
	"database/sql"

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
