package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/rs/zerolog/log"
)

// CreateCatalog inserts a new catalog into the database.
// If the catalog name already exists for the project and tenant, it returns an error.
func (h *hatchCatalogDb) CreateCatalog(ctx context.Context, catalog *models.Catalog) apperrors.Error {
	tenantID, projectID, err := getTenantAndProjectFromContext(ctx)
	if err != nil {
		return err
	}
	catalog.ProjectID = projectID
	if catalog.CatalogID == uuid.Nil {
		catalog.CatalogID = uuid.New()
	}

	query := `
		INSERT INTO catalogs (catalog_id, name, description, info, tenant_id, project_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (name, project_id, tenant_id) DO NOTHING
		RETURNING catalog_id, name;
	`

	// Execute the query directly using h.conn().QueryRowContext
	row := h.conn().QueryRowContext(ctx, query, catalog.CatalogID, catalog.Name, catalog.Description, catalog.Info, tenantID, projectID)
	var insertedCatalogID, insertedName string
	errDb := row.Scan(&insertedCatalogID, &insertedName)
	if errDb != nil {
		if errDb == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("name", catalog.Name).Str("catalog_id", catalog.CatalogID.String()).Msg("catalog already exists")
			return dberror.ErrAlreadyExists.Msg("catalog already exists")
		}
		log.Ctx(ctx).Error().Err(errDb).Str("name", catalog.Name).Str("catalog_id", catalog.CatalogID.String()).Msg("failed to insert catalog")
		return dberror.ErrDatabase.Err(errDb)
	}

	return nil
}

// GetCatalog retrieves a catalog from the database.
// If both catalogID and name are provided, catalogID takes precedence.
func (h *hatchCatalogDb) GetCatalog(ctx context.Context, catalogID uuid.UUID, name string) (*models.Catalog, apperrors.Error) {
	tenantID, projectID, err := getTenantAndProjectFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Construct the query based on input
	query := `
        SELECT catalog_id, name, description, info, project_id
        FROM catalogs
        WHERE tenant_id = $2 AND project_id = $3 AND `

	var row *sql.Row
	if catalogID != uuid.Nil {
		query += "catalog_id = $1;"
		row = h.conn().QueryRowContext(ctx, query, catalogID, tenantID, projectID)
	} else {
		query += "name = $1;"
		row = h.conn().QueryRowContext(ctx, query, name, tenantID, projectID)
	}

	// Scan the result into the catalog model
	var catalog models.Catalog
	errDb := row.Scan(&catalog.CatalogID, &catalog.Name, &catalog.Description, &catalog.Info, &catalog.ProjectID)
	if errDb != nil {
		if errDb == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("name", name).Str("catalog_id", catalogID.String()).Msg("catalog not found")
			return nil, dberror.ErrNotFound.Msg("catalog not found")
		}
		log.Ctx(ctx).Error().Err(errDb).Str("name", name).Str("catalog_id", catalogID.String()).Msg("failed to retrieve catalog")
		return nil, dberror.ErrDatabase.Err(errDb)
	}

	return &catalog, nil
}

// UpdateCatalog updates an existing catalog in the database.
// If both catalogID and name are provided, catalogID takes precedence.
func (h *hatchCatalogDb) UpdateCatalog(ctx context.Context, catalog models.Catalog) apperrors.Error {
	// Retrieve tenant and project IDs from context
	tenantID, projectID, err := getTenantAndProjectFromContext(ctx)
	if err != nil {
		return err
	}

	// Validate input to ensure either catalogID or name is provided
	if catalog.CatalogID == uuid.Nil && catalog.Name == "" {
		log.Ctx(ctx).Error().Msg("catalogID or name must be provided")
		return dberror.ErrInvalidInput.Msg("catalogID or name must be provided")
	}

	// Construct the update query based on the provided input
	query := `
		UPDATE catalogs
		SET description = $4, info = $5
		WHERE tenant_id = $2 AND project_id = $3 AND `

	var row *sql.Row
	if catalog.CatalogID != uuid.Nil {
		query += "catalog_id = $1 RETURNING catalog_id, name;"
		row = h.conn().QueryRowContext(ctx, query, catalog.CatalogID, tenantID, projectID, catalog.Description, catalog.Info)
	} else {
		query += "name = $1 RETURNING catalog_id, name;"
		row = h.conn().QueryRowContext(ctx, query, catalog.Name, tenantID, projectID, catalog.Description, catalog.Info)
	}

	// Scan the updated values
	var updatedCatalogID, updatedName string
	errDb := row.Scan(&updatedCatalogID, &updatedName)
	if errDb != nil {
		if errDb == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("name", catalog.Name).Str("catalog_id", catalog.CatalogID.String()).Msg("catalog not found for update")
			return dberror.ErrNotFound.Msg("catalog not found for update")
		}
		log.Ctx(ctx).Error().Err(errDb).Str("name", catalog.Name).Str("catalog_id", catalog.CatalogID.String()).Msg("failed to update catalog")
		return dberror.ErrDatabase.Err(errDb)
	}

	return nil
}

// DeleteCatalog deletes a catalog from the database.
// If both catalogID and name are provided, catalogID takes precedence.
func (h *hatchCatalogDb) DeleteCatalog(ctx context.Context, catalogID uuid.UUID, name string) apperrors.Error {
	// Retrieve tenant and project IDs from context
	tenantID, projectID, err := getTenantAndProjectFromContext(ctx)
	if err != nil {
		return err
	}

	// Validate input to ensure either catalogID or name is provided
	if catalogID == uuid.Nil && name == "" {
		log.Ctx(ctx).Error().Msg("catalogID or name must be provided")
		return dberror.ErrInvalidInput.Msg("catalogID or name must be provided")
	}

	query := `
		DELETE FROM catalogs
		WHERE tenant_id = $2 AND project_id = $3 AND `

	if catalogID != uuid.Nil {
		query += "catalog_id = $1;"
		_, err := h.conn().ExecContext(ctx, query, catalogID, tenantID, projectID)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("catalog_id", catalogID.String()).Msg("failed to delete catalog")
			return dberror.ErrDatabase.Err(err)
		}
	} else {
		query += "name = $1;"
		_, err := h.conn().ExecContext(ctx, query, name, tenantID, projectID)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("name", name).Msg("failed to delete catalog")
			return dberror.ErrDatabase.Err(err)
		}
	}

	return nil
}
