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
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

/*
func (h *hatchCatalogDb) CreateNamespace(ctx context.Context, ns *models.Namespace) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if ns.Name == "" {
		ns.Name = "default" // Set a default name if empty
		log.Ctx(ctx).Warn().Msg("namespace name is empty, using default 'default'")
	}

	query := `
		INSERT INTO namespaces (name, variant_id, catalog_id, tenant_id, description, info)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (name, variant_id, catalog_id, tenant_id) DO NOTHING;
	`

	result, err := h.conn().ExecContext(ctx, query, ns.Name, ns.VariantID, ns.CatalogID, tenantID, ns.Description, ns.Info)
	if err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		if ok && pgErr.Code == "23514" && pgErr.ConstraintName == "namespaces_name_check" {
			return dberror.ErrInvalidInput.Msg("invalid namespace name")
		}
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	if rowsAffected == 0 {
		return dberror.ErrAlreadyExists.Msg("namespace already exists")
	}

	return nil
}
*/

func (h *hatchCatalogDb) CreateNamespace(ctx context.Context, ns *models.Namespace) (err apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	ns.TenantID = tenantID

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

	err = h.createNamespaceWithTransaction(ctx, ns, tx)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to create namespace")
		return err
	}

	if errStd := tx.Commit(); errStd != nil {
		log.Ctx(ctx).Error().Err(errStd).Msg("failed to commit transaction")
		return dberror.ErrDatabase.Err(errStd)
	}

	return nil
}

func (h *hatchCatalogDb) createNamespaceWithTransaction(ctx context.Context, ns *models.Namespace, tx *sql.Tx) apperrors.Error {
	if ns.Name == "" {
		ns.Name = types.DefaultNamespace
	}
	// Treat empty string as NULL
	description := sql.NullString{String: ns.Description, Valid: ns.Description != ""}

	query := `
		INSERT INTO namespaces (name, variant_id, catalog_id, tenant_id, description, info)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := tx.ExecContext(ctx, query,
		ns.Name,
		ns.VariantID,
		ns.CatalogID,
		ns.TenantID,
		description,
		ns.Info,
	)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			switch {
			case pgErr.Code == "23505":
				return dberror.ErrAlreadyExists.Msg("namespace already exists")
			case pgErr.Code == "23514" && pgErr.ConstraintName == "namespaces_name_check":
				log.Ctx(ctx).Error().Str("name", ns.Name).Msg("invalid namespace name format")
				return dberror.ErrInvalidInput.Msg("invalid namespace name format")
			case pgErr.ConstraintName == "namespaces_variant_id_catalog_id_tenant_id_fkey":
				log.Ctx(ctx).Error().
					Str("variant_id", ns.VariantID.String()).
					Str("catalog_id", ns.CatalogID.String()).
					Msg("variant or catalog not found")
				return dberror.ErrInvalidCatalog
			}
		}
		log.Ctx(ctx).Error().Err(err).Str("name", ns.Name).Msg("failed to insert namespace")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

func (h *hatchCatalogDb) GetNamespace(ctx context.Context, name string, variantID, catalogID uuid.UUID) (*models.Namespace, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	if name == "" {
		return nil, dberror.ErrInvalidInput.Msg("namespace name cannot be empty")
	}

	query := `
		SELECT name, variant_id, catalog_id, tenant_id, description, info
		FROM namespaces
		WHERE name = $1 AND variant_id = $2 AND catalog_id = $3 AND tenant_id = $4
	`

	var ns models.Namespace
	err := h.conn().QueryRowContext(ctx, query, name, variantID, catalogID, tenantID).
		Scan(&ns.Name, &ns.VariantID, &ns.CatalogID, &ns.TenantID, &ns.Description, &ns.Info)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("namespace not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &ns, nil
}

func (h *hatchCatalogDb) UpdateNamespace(ctx context.Context, ns *models.Namespace) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if ns.Name == "" {
		return dberror.ErrInvalidInput.Msg("namespace name cannot be empty")
	}

	query := `
		UPDATE namespaces
		SET description = $5,
		    info = $6,
		    updated_at = NOW()
		WHERE name = $1 AND variant_id = $2 AND catalog_id = $3 AND tenant_id = $4
	`

	result, err := h.conn().ExecContext(ctx, query, ns.Name, ns.VariantID, ns.CatalogID, tenantID, ns.Description, ns.Info)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to update namespace")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("namespace not found")
	}

	return nil
}

func (h *hatchCatalogDb) DeleteNamespace(ctx context.Context, name string, variantID, catalogID uuid.UUID) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if name == "" {
		return dberror.ErrInvalidInput.Msg("namespace name cannot be empty")
	}

	query := `
		DELETE FROM namespaces
		WHERE name = $1 AND variant_id = $2 AND catalog_id = $3 AND tenant_id = $4
	`

	result, err := h.conn().ExecContext(ctx, query, name, variantID, catalogID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("namespace not found")
	}

	return nil
}

func (h *hatchCatalogDb) ListNamespacesByVariant(ctx context.Context, catalogID, variantID uuid.UUID) ([]*models.Namespace, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT name, variant_id, catalog_id, tenant_id, description, info
		FROM namespaces
		WHERE catalog_id = $1 AND variant_id = $2 AND tenant_id = $3
		ORDER BY name ASC
	`

	rows, err := h.conn().QueryContext(ctx, query, catalogID, variantID, tenantID)
	if err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	var result []*models.Namespace

	for rows.Next() {
		var ns models.Namespace
		err := rows.Scan(&ns.Name, &ns.VariantID, &ns.CatalogID, &ns.TenantID, &ns.Description, &ns.Info)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to scan namespace row")
			return nil, dberror.ErrDatabase.Err(err)
		}
		result = append(result, &ns)
	}

	if err := rows.Err(); err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}

	return result, nil
}
