package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

// The Collections interface functions are a shim on top of schema directory.  This would allow for a different implementation
// in future, if necessary.

func (h *hatchCatalogDb) UpsertCollection(ctx context.Context, c *models.Collection) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	c.TenantID = tenantID

	dir, err := h.getValuesDirectory(ctx, c.RepoID, c.VariantID)
	if err != nil {
		return err
	}

	if !isValidPath(c.CollectionSchema) {
		return dberror.ErrInvalidInput.Msg("invalid collection schema")
	}

	err = h.AddOrUpdateObjectByPath(ctx,
		types.CatalogObjectTypeCatalogCollection,
		dir,
		c.Path,
		models.ObjectRef{
			Hash:       c.Hash,
			BaseSchema: c.CollectionSchema,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (h *hatchCatalogDb) GetCollection(ctx context.Context, path string, repoID, variantID uuid.UUID) (*models.Collection, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	dir, err := h.getValuesDirectory(ctx, repoID, variantID)
	if err != nil {
		return nil, err
	}

	objRef, err := h.GetObjectRefByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, path)
	if err != nil {
		return nil, err
	}

	return &models.Collection{
		Path:             path,
		Hash:             objRef.Hash,
		CollectionSchema: objRef.BaseSchema,
		RepoID:           repoID,
		VariantID:        variantID,
		TenantID:         tenantID,
	}, nil
}

func (h *hatchCatalogDb) GetCollectionObject(ctx context.Context, path string, repoID, variantID uuid.UUID) (*models.CatalogObject, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	dir, err := h.getValuesDirectory(ctx, repoID, variantID)
	if err != nil {
		return nil, err
	}

	return h.LoadObjectByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, path)
}

func (h *hatchCatalogDb) UpdateCollection(ctx context.Context, c *models.Collection) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if !isValidPath(c.CollectionSchema) {
		return dberror.ErrInvalidInput.Msg("invalid collection schema")
	}

	dir, err := h.getValuesDirectory(ctx, c.RepoID, c.VariantID)
	if err != nil {
		return err
	}

	objRef, err := h.GetObjectRefByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, c.Path)
	if err != nil {
		return err
	}
	objRef.Hash = c.Hash
	objRef.BaseSchema = c.CollectionSchema
	err = h.AddOrUpdateObjectByPath(ctx,
		types.CatalogObjectTypeCatalogCollection,
		dir,
		c.Path,
		*objRef,
	)
	if err != nil {
		return err
	}
	return nil
}

func (h *hatchCatalogDb) DeleteCollection(ctx context.Context, path string, repoID, variantID uuid.UUID) (string, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", dberror.ErrMissingTenantID
	}

	dir, err := h.getValuesDirectory(ctx, repoID, variantID)
	if err != nil {
		return "", err
	}

	deletedHash, err := h.DeleteObjectByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir, path)
	if err != nil {
		return "", err
	}

	return string(deletedHash), nil
}

func (h *hatchCatalogDb) HasReferencesToCollectionSchema(ctx context.Context, collectionSchema string, repoID, variantID uuid.UUID) (bool, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return false, dberror.ErrMissingTenantID
	}

	dir, err := h.getValuesDirectory(ctx, repoID, variantID)
	if err != nil {
		return false, err
	}

	query := `
		SELECT 1
		FROM values_directory
		WHERE jsonb_path_query_array(directory, '$.*.base_schema') @> to_jsonb($1::text)
		AND directory_id = $2
		AND tenant_id = $3
		LIMIT 1;
	`
	var exists bool // we'll probably just hit the ErrNoRows case in case of false
	dberr := h.conn().QueryRowContext(ctx, query, collectionSchema, dir, tenantID).Scan(&exists)
	if dberr != nil {
		if dberr == sql.ErrNoRows {
			return false, nil
		}
		return false, dberror.ErrDatabase.Err(dberr)
	}
	return exists, nil
}

func (h *hatchCatalogDb) getValuesDirectory(ctx context.Context, repoId, variantId uuid.UUID) (uuid.UUID, apperrors.Error) {
	var dir uuid.UUID
	if repoId != variantId {
		w, err := h.GetWorkspace(ctx, repoId)
		if err != nil {
			return uuid.Nil, err
		}
		dir = w.ValuesDir
	} else {
		v, err := h.GetVersion(ctx, 1, variantId)
		if err != nil {
			return uuid.Nil, dberror.ErrDatabase.Err(err)
		}
		dir = v.ValuesDir
	}
	return dir, nil
}
