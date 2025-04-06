package catalogmanager

import (
	"context"
	"encoding/json"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/resource"
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
	err := json.Unmarshal([]byte(rsrcJson), &version)
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

	rm, apperr := resource.NewV1ResourceManager(ctx, []byte(rsrcJson), schemamanager.WithValidation())
	if apperr != nil {
		return nil, apperr
	}
	return rm, nil
}
