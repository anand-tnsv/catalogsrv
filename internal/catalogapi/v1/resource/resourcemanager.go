package resource

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schemamanager"
	_ "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/v1/customvalidators"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/v1/parameter"
)

type V1ResourceManager struct {
	resourceSchema   ResourceSchema
	parameterManager parameter.V1ParameterManager
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
		return nil, apierrors.ErrInvalidVersion
	}
	if o.Validate {
		ves := rs.Validate()
		if ves != nil {
			return nil, apierrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}
	if rs.Kind == "Parameter" {
		pm, err := parameter.NewV1ParameterManager(ctx, rs.Version, rsrcJson, options...)
		if err != nil {
			return nil, err
		}
		return &V1ResourceManager{
			resourceSchema:   *rs,
			parameterManager: *pm,
		}, nil
	}
	return nil, apierrors.ErrInvalidKind
}

func (rm *V1ResourceManager) Version() string {
	return rm.resourceSchema.Version
}

func (rm *V1ResourceManager) Kind() string {
	return rm.resourceSchema.Kind
}

func (rm *V1ResourceManager) ParameterManager() schemamanager.ParameterManager {
	return &rm.parameterManager
}

func (rm *V1ResourceManager) CollectionManager() schemamanager.CollectionManager {
	return nil
}
