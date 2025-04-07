package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/resource"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/rs/zerolog/log"
)

type VersionHeader struct {
	Version string `json:"version"`
}

func NewResource(ctx context.Context, rsrcJson []byte) (schemamanager.ResourceManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}
	// get the version
	var version VersionHeader
	err := json.Unmarshal(rsrcJson, &version)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal version header")
		return nil, validationerrors.ErrSchemaValidation
	}
	if version.Version == "" {
		return nil, validationerrors.ErrSchemaValidation.Msg(schemaerr.ErrMissingRequiredAttribute("version").Error())
	}
	// validate the version
	if version.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}

	return resource.NewV1ResourceManager(ctx, []byte(rsrcJson), schemamanager.WithValidation())
}

func SaveResource(ctx context.Context, s *schemastore.SchemaStorageRepresentation, errorIfExists ...bool) apperrors.Error {
	if s == nil {
		return validationerrors.ErrEmptySchema
	}
	data, err := s.Serialize()
	if err != nil {
		return validationerrors.ErrSchemaSerialization
	}
	obj := models.CatalogObject{
		Type:    s.Type,
		Version: s.Version,
		Data:    data,
		Hash:    s.GetHash(),
	}
	// Save obj to the database
	dberr := db.DB(ctx).CreateCatalogObject(ctx, &obj)
	if dberr != nil {
		if errors.Is(dberr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("catalog object already exists")
			if len(errorIfExists) > 0 && errorIfExists[0] {
				return ErrAlreadyExists.Err(dberr)
			}
		} else {
			log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
			return dberr
		}
	}
	return nil
}

func LoadResource(ctx context.Context, hash string, m *schemamanager.ResourceMetadata) (schemamanager.ResourceManager, apperrors.Error) {
	if hash == "" {
		return nil, dberror.ErrInvalidInput.Msg("hash cannot be empty")
	}

	obj, err := db.DB(ctx).GetCatalogObject(ctx, hash)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrResourceNotFound.Err(err)
		}
		return nil, ErrUnableToLoadResource.Err(err)
	}

	s := &schemastore.SchemaStorageRepresentation{}
	// we'll get the data from the object and not the table
	if err := json.Unmarshal(obj.Data, s); err != nil {
		return nil, ErrUnableToLoadResource.Err(err).Msg("failed to de-serialize catalog object data")
	}

	if s.Type != obj.Type {
		log.Ctx(ctx).Error().Str("Hash", hash).Msg("type mismatch when loading resource")
	}

	if s.Version != obj.Version {
		log.Ctx(ctx).Error().Str("Hash", hash).Msg("version mismatch when loading resource")
	}

	return resource.LoadV1ResourceManager(ctx, s, m)
}
