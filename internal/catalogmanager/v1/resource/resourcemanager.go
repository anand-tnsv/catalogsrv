package resource

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/collection"
	_ "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/customvalidators"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/parameter"
)

type V1ResourceManager struct {
	resourceSchema    *ResourceSchema
	parameterManager  *parameter.V1ParameterManager
	collectionManager *collection.V1CollectionManager
}

var _ schemamanager.ResourceManager = &V1ResourceManager{} // Ensure V1ResourceManager implements schemamanager.ResourceManager

func NewV1ResourceManager(ctx context.Context, rsrcJson []byte, options ...schemamanager.Options) (*V1ResourceManager, apperrors.Error) {
	o := schemamanager.OptionsConfig{}
	for _, option := range options {
		option(&o)
	}

	rs, err := ReadResourceSchema(string(rsrcJson))
	if err != nil {
		return nil, err
	}

	if rs.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}
	if o.Validate {
		ves := rs.Validate()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}
	rm := &V1ResourceManager{
		resourceSchema: rs,
	}
	// Initialize the appropriate manager based on the kind
	switch rs.Kind {
	case "Parameter":
		if rm.parameterManager, err = parameter.NewV1ParameterManager(ctx, rs.Version, rsrcJson, options...); err != nil {
			return nil, err
		}
	case "Collection":
		if rm.collectionManager, err = collection.NewV1CollectionManager(ctx, rs.Version, rsrcJson, options...); err != nil {
			return nil, err
		}
	default:
		return nil, validationerrors.ErrInvalidKind
	}

	return rm, nil
}

func (rm *V1ResourceManager) Version() string {
	return rm.resourceSchema.Version
}

func (rm *V1ResourceManager) Kind() string {
	return rm.resourceSchema.Kind
}

func (rm *V1ResourceManager) ParameterManager() schemamanager.ParameterManager {
	return rm.parameterManager
}

func (rm *V1ResourceManager) CollectionManager() schemamanager.CollectionManager {
	return rm.collectionManager
}
