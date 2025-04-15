package postgresql

import (
	"context"
	"database/sql"

	"github.com/golang/snappy"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/rs/zerolog/log"
)

func (h *hatchCatalogDb) CreateCatalogObject(ctx context.Context, obj *models.CatalogObject) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if obj.Hash == "" {
		return dberror.ErrInvalidInput.Msg("hash cannot be empty")
	}
	if obj.Type == "" {
		return dberror.ErrInvalidInput.Msg("type cannot be empty")
	}
	if obj.Version == "" {
		return dberror.ErrInvalidInput.Msg("version cannot be empty")
	}
	if len(obj.Data) == 0 {
		return dberror.ErrInvalidInput.Msg("data cannot be nil")
	}

	// snappy compress the data
	dataZ := snappy.Encode(nil, obj.Data)
	log.Ctx(ctx).Debug().Msgf("raw: %d, compressed: %d", len(obj.Data), len(dataZ))

	// Insert the catalog object into the database
	query := `
		INSERT INTO catalog_objects (hash, type, version, tenant_id, data)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (hash, tenant_id) DO NOTHING;
	`
	result, err := h.conn().ExecContext(ctx, query, obj.Hash, obj.Type, obj.Version, tenantID, dataZ)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	// Check if the row was inserted
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	// If no rows were affected, it means the object already exists
	if rowsAffected == 0 {
		return dberror.ErrAlreadyExists.Msg("catalog object already exists")
	}

	return nil
}

func (h *hatchCatalogDb) GetCatalogObject(ctx context.Context, hash string) (*models.CatalogObject, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	if hash == "" {
		return nil, dberror.ErrInvalidInput.Msg("hash cannot be empty")
	}

	// Query to select catalog object based on composite key (hash, tenant_id)
	query := `
		SELECT hash, type, version, tenant_id, data
		FROM catalog_objects
		WHERE hash = $1 AND tenant_id = $2
	`
	row := h.conn().QueryRowContext(ctx, query, hash, tenantID)

	var obj models.CatalogObject

	// Scan the result into obj fields
	err := row.Scan(&obj.Hash, &obj.Type, &obj.Version, &obj.TenantID, &obj.Data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("catalog object not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	// Uncompress the data
	obj.Data, err = snappy.Decode(nil, obj.Data)
	if err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &obj, nil
}

func (h *hatchCatalogDb) DeleteCatalogObject(ctx context.Context, hash string) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if hash == "" {
		return dberror.ErrInvalidInput.Msg("hash cannot be empty")
	}

	// Query to delete catalog object based on composite key (hash, tenant_id)
	query := `
		DELETE FROM catalog_objects
		WHERE hash = $1 AND tenant_id = $2
	`
	result, err := h.conn().ExecContext(ctx, query, hash, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	// Check if the row was deleted
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	// If no rows were affected, it means the object does not exist
	if rowsAffected == 0 {
		return dberror.ErrNotFound.Msg("catalog object not found")
	}

	return nil
}
