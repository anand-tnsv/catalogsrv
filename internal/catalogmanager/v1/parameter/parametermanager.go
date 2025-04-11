package parameter

import (
	"context"
	"encoding/json"

	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager/datatyperegistry"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
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
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read parameter schema")
	}
	if o.Validate {
		ves := ps.Validate()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	// load the parameter spec
	loader := datatyperegistry.GetLoader(datatyperegistry.DataTypeKey{
		Type:    ps.Spec.DataType,
		Version: version,
	})

	if loader == nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(schemaerr.ErrUnsupportedDataType("spec.dataType", ps.Spec.DataType).Error())
	}

	js, err := json.Marshal(ps.Spec)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read parameter spec")
	}
	parameter, apperr := loader(js)
	if apperr != nil {
		return nil, apperr
	}
	if o.Validate {
		ves := parameter.ValidateSpec()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	return &V1ParameterManager{
		version:         version,
		parameterSchema: *ps,
		parameter:       parameter,
	}, nil
}

func (pm *V1ParameterManager) DataType() string {
	return pm.parameterSchema.Spec.DataType
}

func (pm *V1ParameterManager) Default() interface{} {
	return pm.parameter.DefaultValue()
}

func (pm *V1ParameterManager) ValidateValue(value any) apperrors.Error {
	return pm.parameter.ValidateValue(value)
}

func (pm *V1ParameterManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: pm.version,
		Type:    types.CatalogObjectTypeParameterSchema,
	}
	s.Schema, _ = json.Marshal(pm.parameterSchema.Spec)
	return &s
}
