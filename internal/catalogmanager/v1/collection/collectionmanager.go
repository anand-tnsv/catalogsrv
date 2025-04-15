package collection

import (
	"context"
	"encoding/json"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
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
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read collection schema")
	}

	// Just to ensure we have consistent version throughout, let's update cs with the version
	cs.Version = version

	if o.Validate {
		ves := cs.Validate()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	if o.ValidateDependencies {
		_, ves := cs.ValidateDependencies(ctx, o.ObjectLoaders, schemamanager.ObjectReferences{})
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	if o.SetDefaultValues {
		cs.SetDefaultValues(ctx)
	}

	if o.ParamValues != nil {
		err := json.Unmarshal(o.ParamValues, &cs.Values)
		if err != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg("failed to read parameter values")
		}
	}

	return &V1CollectionManager{
		version:          version,
		collectionSchema: *cs,
	}, nil
}

func (cm *V1CollectionManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: cm.version,
		Type:    types.CatalogObjectTypeCollectionSchema,
	}
	s.Values, _ = json.Marshal(cm.collectionSchema.Values)
	s.Schema, _ = json.Marshal(cm.collectionSchema.Spec)
	return &s
}

func (cm *V1CollectionManager) ParameterNames() []string {
	return cm.collectionSchema.ParameterNames()
}

func (cm *V1CollectionManager) ParametersWithSchema(schemaName string) []schemamanager.ParameterSpec {
	return cm.collectionSchema.ParametersWithSchema(schemaName)
}

func (cm *V1CollectionManager) ValidateDependencies(ctx context.Context, loaders schemamanager.ObjectLoaders, existingRefs schemamanager.ObjectReferences) (schemamanager.ObjectReferences, apperrors.Error) {
	refs, ves := cm.collectionSchema.ValidateDependencies(ctx, loaders, existingRefs)
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	return refs, nil
}

func (cm *V1CollectionManager) ValidateValue(ctx context.Context, loaders schemamanager.ObjectLoaders, param string, value types.NullableAny) apperrors.Error {
	ves := cm.collectionSchema.ValidateValue(ctx, loaders, param, value)
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	return nil
}

func (cm *V1CollectionManager) GetValue(ctx context.Context, param string) schemamanager.ParamValue {
	return cm.collectionSchema.GetValue(ctx, param)
}

func (cm *V1CollectionManager) GetValues(ctx context.Context) map[string]schemamanager.ParamValue {
	return cm.collectionSchema.Values
}

func (cm *V1CollectionManager) SetValue(ctx context.Context, param string, value types.NullableAny) apperrors.Error {
	err := cm.collectionSchema.SetValue(ctx, param, value)
	if err != nil {
		return validationerrors.ErrSchemaValidation.Msg(err.Error())
	}
	return nil
}

func (cm *V1CollectionManager) SetDefaultValues(ctx context.Context) {
	cm.collectionSchema.SetDefaultValues(ctx)
}
