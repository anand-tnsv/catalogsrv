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
		_, ves := cs.ValidateDependencies(ctx, o.ObjectLoaders)
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}

	if o.SetDefaultValues {
		cs.SetDefaultValues(ctx)
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
	s.Schema, _ = json.Marshal(cm.collectionSchema.Spec)
	return &s
}

func (cm *V1CollectionManager) ParameterSchemaReferences() []string {
	var refs []string
	for _, p := range cm.collectionSchema.Spec.Parameters {
		refs = append(refs, p.Schema)
	}
	return refs
}

func (cm *V1CollectionManager) ValidateDependencies(ctx context.Context, loaders schemamanager.ObjectLoaders) (schemamanager.ParameterReferences, apperrors.Error) {
	refs, ves := cm.collectionSchema.ValidateDependencies(ctx, loaders)
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

func (cm *V1CollectionManager) SetValue(ctx context.Context, param string, value types.NullableAny) apperrors.Error {
	err := cm.collectionSchema.SetValue(ctx, param, value)
	if err != nil {
		return validationerrors.ErrSchemaValidation.Msg(err.Error())
	}
	return nil
}
