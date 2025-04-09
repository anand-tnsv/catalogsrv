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

// CreateVariant creates a new variant in the database.
// It generates a new UUID for the variant ID and sets the project ID based on the context.
// If a variant with the same name and catalog ID already exists, the insertion is skipped.
// Returns an error if the variant already exists, the variant name format is invalid,
// the catalog ID is invalid, or there is a database error.
func (h *hatchCatalogDb) CreateVariant(ctx context.Context, variant *models.Variant) apperrors.Error {
	// Generate a new UUID for the variant ID
	variantID := uuid.New()

	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		INSERT INTO variants (variant_id, name, description, info, catalog_id, tenant_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (name, catalog_id) DO NOTHING
		RETURNING variant_id, name;
	`

	// Execute the query directly using h.conn().QueryRowContext
	row := h.conn().QueryRowContext(ctx, query, variantID, variant.Name, variant.Description, variant.Info, variant.CatalogID, tenantID)
	var insertedVariantID uuid.UUID
	var insertedName string
	err := row.Scan(&insertedVariantID, &insertedName)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("name", variant.Name).Str("variant_id", variant.VariantID.String()).Msg("variant already exists")
			return dberror.ErrAlreadyExists.Msg("variant already exists")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23514" && pgErr.ConstraintName == "variants_name_check" { // Check constraint violation code and specific constraint name
				log.Ctx(ctx).Error().Str("name", variant.Name).Msg("invalid variant name format")
				return dberror.ErrInvalidInput.Msg("invalid variant name format")
			}
			if pgErr.ConstraintName == "variants_catalog_id_fkey" { // Foreign key constraint violation
				log.Ctx(ctx).Info().Str("catalog_id", variant.CatalogID.String()).Msg("catalog not found")
				return dberror.ErrInvalidCatalog
			}
		}
		log.Ctx(ctx).Error().Err(err).Str("name", variant.Name).Str("variant_id", variantID.String()).Msg("failed to insert variant")
		return dberror.ErrDatabase.Err(err)
	}
	variant.VariantID = insertedVariantID
	return nil
}

// GetVariant retrieves a variant from the database based on the variant ID or name.
// If both variantID and name are provided, variantID takes precedence.
// Returns the variant if found, or an error if the variant is not found or there is a database error.
func (h *hatchCatalogDb) GetVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) (*models.Variant, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	var query string
	var row *sql.Row

	if variantID != uuid.Nil {
		query = `
			SELECT variant_id, name, description, info, catalog_id
			FROM variants
			WHERE variant_id = $1 AND catalog_id = $2 AND tenant_id = $3;
		`
		row = h.conn().QueryRowContext(ctx, query, variantID, catalogID, tenantID)
	} else if name != "" {
		query = `
			SELECT variant_id, name, description, info, catalog_id
			FROM variants
			WHERE name = $1 AND catalog_id = $2 AND tenant_id = $3;
		`
		row = h.conn().QueryRowContext(ctx, query, name, catalogID, tenantID)
	} else {
		log.Ctx(ctx).Error().Msg("either variant ID or name must be provided")
		return nil, dberror.ErrInvalidInput.Msg("either variant ID or name must be provided")
	}

	variant := &models.Variant{}
	err := row.Scan(&variant.VariantID, &variant.Name, &variant.Description, &variant.Info, &variant.CatalogID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Msg("variant not found")
			return nil, dberror.ErrNotFound.Msg("variant not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve variant")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return variant, nil
}

// UpdateVariant updates an existing variant in the database based on the variant ID or name.
// If both variantID and name are provided, variantID takes precedence.
// The VariantID and CatalogID fields cannot be updated.
// Returns an error if the variant is not found, the variant name already exists for the given catalog ID,
// the variant name format is invalid, or there is a database error.
func (h *hatchCatalogDb) UpdateVariant(ctx context.Context, variantID uuid.UUID, name string, updatedVariant *models.Variant) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	var query string
	var row *sql.Row

	if variantID != uuid.Nil {
		query = `
			UPDATE variants
			SET name = $1, description = $2, info = $3
			WHERE variant_id = $4 AND catalog_id = $5 AND tenant_id = $6
			RETURNING variant_id;
		`
		row = h.conn().QueryRowContext(ctx, query, updatedVariant.Name, updatedVariant.Description, updatedVariant.Info, variantID, updatedVariant.CatalogID, tenantID)
	} else if name != "" {
		query = `
			UPDATE variants
			SET name = $1, description = $2, info = $3
			WHERE name = $4 AND catalog_id = $5 AND tenant_id = $6
			RETURNING variant_id;
		`
		row = h.conn().QueryRowContext(ctx, query, updatedVariant.Name, updatedVariant.Description, updatedVariant.Info, name, updatedVariant.CatalogID, tenantID)
	} else {
		log.Ctx(ctx).Error().Msg("either variant ID or name must be provided")
		return dberror.ErrInvalidInput.Msg("either variant ID or name must be provided")
	}

	var returnedVariantID uuid.UUID
	err := row.Scan(&returnedVariantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Msg("variant not found or no changes made")
			return dberror.ErrNotFound.Msg("variant not found or no changes made")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "variants_name_catalog_id_key" { // Unique constraint violation
				log.Ctx(ctx).Error().Msg("variant name already exists for the given catalog_id")
				return dberror.ErrAlreadyExists.Msg("variant name already exists for the given catalog_id")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "variants_name_check" { // Check constraint violation code and specific constraint name
				log.Ctx(ctx).Error().Str("name", updatedVariant.Name).Msg("invalid variant name format")
				return dberror.ErrInvalidInput.Msg("invalid variant name format")
			}
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update variant")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

// DeleteVariant deletes a variant from the database based on the variant ID or name.
// If both variantID and name are provided, variantID takes precedence.
// Returns an error if the variant is not found or there is a database error.
func (h *hatchCatalogDb) DeleteVariant(ctx context.Context, catalogID, variantID uuid.UUID, name string) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if catalogID == uuid.Nil {
		log.Ctx(ctx).Error().Msg("catalog ID is required")
		return dberror.ErrInvalidInput.Msg("catalog ID is required")
	}

	if variantID == uuid.Nil && name == "" {
		log.Ctx(ctx).Error().Msg("either variant ID or name must be provided")
		return dberror.ErrInvalidInput.Msg("either variant ID or name must be provided")
	}

	var query string
	var err error
	var result sql.Result

	if variantID != uuid.Nil {
		query = `
			DELETE FROM variants
			WHERE variant_id = $1 AND catalog_id = $2 AND tenant_id = $3;
		`
		result, err = h.conn().ExecContext(ctx, query, variantID, catalogID, tenantID)
	} else {
		query = `
			DELETE FROM variants
			WHERE name = $1 AND catalog_id = $2 AND tenant_id = $3;
		`
		result, err = h.conn().ExecContext(ctx, query, name, catalogID, tenantID)
	}

	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete variant")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Str("variant_id", variantID.String()).Str("variant_name", name).Str("catalog_id", catalogID.String()).Str("tenant_id", string(tenantID)).Msg("variant not found")
	}

	return nil
}
