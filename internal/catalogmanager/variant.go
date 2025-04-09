package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/rs/zerolog/log"
)

type variantSchema struct {
	Version  string          `json:"version" validate:"required"`
	Kind     string          `json:"kind" validate:"required,kindValidator"`
	Metadata variantMetadata `json:"metadata" validate:"required"`
}

type variantMetadata struct {
	Name        string `json:"name" validate:"required,nameFormatValidator"`
	Catalog     string `json:"catalog" validate:"required,nameFormatValidator"`
	Description string `json:"description"`
}

type variantManager struct {
	v       models.Variant
	catalog string
}

var _ schemamanager.VariantManager = (*variantManager)(nil)

func NewVariantManager(ctx context.Context, rsrcJson []byte, name string, catalog string) (schemamanager.VariantManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(rsrcJson) == 0 {
		return nil, ErrInvalidSchema
	}

	vs := &variantSchema{}
	if err := json.Unmarshal(rsrcJson, vs); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}
	if vs.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}
	if vs.Kind != "Variant" {
		return nil, validationerrors.ErrInvalidKind
	}

	// replace name and catalog if not empty
	if name != "" {
		if !schemavalidator.ValidateObjectName(name) {
			return nil, validationerrors.ErrInvalidNameFormat
		}
		vs.Metadata.Name = name
	}

	if catalog != "" {
		if !schemavalidator.ValidateObjectName(catalog) {
			return nil, ErrInvalidCatalog
		}
		vs.Metadata.Catalog = catalog
	}

	// retrieve the catalogID
	catalogID, err := db.DB(ctx).GetCatalogIDByName(ctx, vs.Metadata.Catalog)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, err
	}

	v := models.Variant{
		Name:        vs.Metadata.Name,
		Description: vs.Metadata.Description,
		CatalogID:   catalogID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	}

	return &variantManager{
		v:       v,
		catalog: vs.Metadata.Catalog,
	}, nil
}

func (vm *variantManager) ID() uuid.UUID {
	return vm.v.VariantID
}

func (vm *variantManager) Name() string {
	return vm.v.Name
}

func (vm *variantManager) Description() string {
	return vm.v.Description
}

func (vm *variantManager) CatalogID() uuid.UUID {
	return vm.v.CatalogID
}

func (vm *variantManager) Catalog() string {
	return vm.catalog
}

func LoadVariantManagerByName(ctx context.Context, catalogID uuid.UUID, name string) (schemamanager.VariantManager, apperrors.Error) {
	if catalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	v, err := db.DB(ctx).GetVariant(ctx, catalogID, uuid.Nil, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrVariantNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
		return nil, err
	}
	return &variantManager{
		v: *v,
	}, nil
}

func (cv *variantManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateVariant(ctx, &cv.v)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.Msg("variant already exists")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create variant")
		return ErrAlreadyExists.Msg("variant already exists")
	}
	return nil
}

func DeleteVariant(ctx context.Context, catalogID, variantID uuid.UUID, name string) apperrors.Error {
	err := db.DB(ctx).DeleteVariant(ctx, catalogID, variantID, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrVariantNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete catalog")
		return err
	}
	return nil
}

// TODO Handle base variant and copy of data
