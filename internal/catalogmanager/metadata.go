package catalogmanager

import (
	"context"
	"encoding/json"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/rs/zerolog/log"
)

// preventing undefined use warnings
var _ = setMetadata
var _ = getMetadata

func getMetadata(ctx context.Context, rsrcJson []byte) (schemamanager.ObjectMetadata, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return schemamanager.ObjectMetadata{}, validationerrors.ErrEmptySchema
	}

	var rs struct {
		VersionHeader
		Metadata schemamanager.ObjectMetadata `json:"metadata"`
	}
	err := json.Unmarshal(rsrcJson, &rs)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
		return schemamanager.ObjectMetadata{}, validationerrors.ErrSchemaValidation
	}

	return rs.Metadata, nil
}

func setMetadata(rsrcJson []byte, metadata schemamanager.ObjectMetadata) ([]byte, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	var fullMap map[string]json.RawMessage // parse only the first level elements
	if err := json.Unmarshal(rsrcJson, &fullMap); err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal resource schema")
	}

	j, err := json.Marshal(metadata)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal metadata")
	}
	fullMap["metadata"] = j

	rs, err := json.Marshal(fullMap)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal resource schema")
	}

	return rs, nil
}
