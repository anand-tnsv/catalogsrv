package collection

import (
	"context"
	"encoding/json"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schemamanager"
)

type V1CollectionManager struct {
	version          string
	collectionSchema CollectionSchema
}

func NewV1CollectionManager(ctx context.Context, version string, rsrcJson []byte, options ...schemamanager.Options) (*V1CollectionManager, apperrors.Error) {
	o := schemamanager.OptionsConfig{}
	for _, option := range options {
		option(&o)
	}

	// Read the collection schema
	cs := &CollectionSchema{}
	err := json.Unmarshal(rsrcJson, cs)
	if err != nil {
		return nil, apierrors.ErrSchemaValidation.Msg("failed to read collection schema")
	}

	// Just to ensure we have consistent version throughout, let's update cs with the version
	cs.Version = version

	if o.Validate {
		ves := cs.Validate()
		if ves != nil {
			return nil, apierrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	return &V1CollectionManager{
		version:          version,
		collectionSchema: *cs,
	}, nil
}
