package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/rs/zerolog/log"
)

type catalogSchema struct {
	Version  string          `json:"version" validate:"required"`
	Kind     string          `json:"kind" validate:"required,kindValidator"`
	Metadata catalogMetadata `json:"metadata" validate:"required"`
}

type catalogMetadata struct {
	Name        string `json:"name" validate:"required,nameFormatValidator"`
	Description string `json:"description"`
}

type catalogManager struct {
	c models.Catalog
}

var _ schemamanager.CatalogManager = (*catalogManager)(nil)

func NewCatalogManager(ctx context.Context, rsrcJson []byte, name string) (schemamanager.CatalogManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(rsrcJson) == 0 {
		return nil, ErrInvalidSchema
	}

	cs := &catalogSchema{}
	if err := json.Unmarshal(rsrcJson, cs); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}
	if cs.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}
	if cs.Kind != "Catalog" {
		return nil, validationerrors.ErrInvalidKind
	}

	c := models.Catalog{
		Name:        cs.Metadata.Name,
		Description: cs.Metadata.Description,
		ProjectID:   projectID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
	}

	return &catalogManager{
		c: c,
	}, nil
}

func (cm *catalogManager) ID() uuid.UUID {
	return cm.c.CatalogID
}

func (cm *catalogManager) Name() string {
	return cm.c.Name
}

func (cm *catalogManager) Description() string {
	return cm.c.Description
}

func LoadCatalogManagerByName(ctx context.Context, name string) (schemamanager.CatalogManager, apperrors.Error) {
	c, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, err
	}
	return &catalogManager{
		c: *c,
	}, nil
}

func (cm *catalogManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateCatalog(ctx, &cm.c)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.Msg("catalog already exists")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create catalog")
		return err
	}
	return nil
}

func DeleteCatalogByName(ctx context.Context, name string) apperrors.Error {
	err := db.DB(ctx).DeleteCatalog(ctx, uuid.Nil, name)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete catalog")
		return err
	}
	return nil
}
