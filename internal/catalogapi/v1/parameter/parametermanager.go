package parameter

import (
	"context"
	"encoding/json"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schemamanager/datatyperegistry"
	"github.com/mugiliam/hatchcatalogsrv/internal/types"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
)

type V1ParameterManager struct {
	version         string
	parameterSchema ParameterSchema
	parameter       schemamanager.Parameter
}

var _ schemamanager.ParameterManager = &V1ParameterManager{} // Ensure V1ParameterManager implements schemamanager.ParameterManager

func NewV1ParameterManager(ctx context.Context, version string, rsrcJson []byte, options ...schemamanager.Options) (*V1ParameterManager, apperrors.Error) {
	o := schemamanager.OptionsConfig{}
	for _, option := range options {
		option(&o)
	}

	// Read the parameter schema
	ps := &ParameterSchema{}
	err := json.Unmarshal(rsrcJson, ps)
	if err != nil {
		return nil, apierrors.ErrSchemaValidation.Msg("failed to read parameter schema")
	}
	if o.Validate {
		ves := ps.Validate()
		if ves != nil {
			return nil, apierrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	// load the parameter spec
	loader := datatyperegistry.GetLoader(datatyperegistry.DataTypeKey{
		Type:    ps.Spec.DataType,
		Version: version,
	})

	if loader == nil {
		return nil, apierrors.ErrSchemaValidation.Msg(schemaerr.ErrUnsupportedDataType("spec.dataType", ps.Spec.DataType).Error())
	}

	js, err := json.Marshal(ps.Spec)
	if err != nil {
		return nil, apierrors.ErrSchemaValidation.Msg("failed to read parameter spec")
	}
	parameter, apperr := loader(js)
	if apperr != nil {
		return nil, apperr
	}
	if o.Validate {
		ves := parameter.ValidateSpec()
		if ves != nil {
			return nil, apierrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	return &V1ParameterManager{
		version:         version,
		parameterSchema: *ps,
		parameter:       parameter,
	}, nil
}

func (pm *V1ParameterManager) Name() string {
	return pm.parameterSchema.Metadata.Name
}

func (pm *V1ParameterManager) Catalog() string {
	return pm.parameterSchema.Metadata.Catalog
}

func (pm *V1ParameterManager) Path() string {
	return pm.parameterSchema.Metadata.Path
}

func (pm *V1ParameterManager) DataType() string {
	return pm.parameterSchema.Spec.DataType
}

func (pm *V1ParameterManager) Default() interface{} {
	return pm.parameter.DefaultValue()
}

func (pm *V1ParameterManager) Validate(value any) apperrors.Error {
	return pm.parameter.ValidateValue(value)
}

func (pm *V1ParameterManager) StorageRepresentation() schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: pm.version,
		Type:    types.CatalogObjecTypeParameterSchema,
	}
	s.Schema, _ = json.Marshal(pm.parameterSchema.Spec)
	return s
}
