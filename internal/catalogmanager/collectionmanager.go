package catalogmanager

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type collectionSchema struct {
	Version  string                    `json:"version" validate:"required"`
	Kind     string                    `json:"kind" validate:"required,oneof=Collection"`
	Metadata CollectionMetadata        `json:"metadata" validate:"required"`
	Spec     collectionSpec            `json:"spec" validate:"required"`
	Values   schemamanager.ParamValues `json:"-"`
}

type CollectionMetadata struct {
	Name        string               `json:"name" validate:"required,nameFormatValidator"`
	Catalog     string               `json:"catalog" validate:"required,resourceNameValidator"`
	Variant     types.NullableString `json:"variant" validate:"required,resourceNameValidator"`
	Path        string               `json:"path" validate:"required,resourcePathValidator"`
	Description string               `json:"description"`
}

type collectionSpec struct {
	Schema string                       `json:"schema" validate:"required,nameFormatValidator"`
	Values map[string]types.NullableAny `json:"values"`
}

func (cs *collectionSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(cs)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(cs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "oneof":
			ves = append(ves, schemaerr.ErrInvalidFieldSchema(jsonFieldName, e.Value().(string)))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		case "catalogVersionValidator":
			ves = append(ves, schemaerr.ErrInvalidCatalogVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

type collectionManager struct {
	schema collectionSchema                      // schema for the collection
	csm    schemamanager.CollectionSchemaManager // collection schema manager
}

func (cm *collectionManager) Schema() string {
	return cm.schema.Spec.Schema
}

func (cm *collectionManager) CollectionSchema() []byte {
	b, _ := json.Marshal(cm.schema.Spec)
	return b
}

func (cm *collectionManager) Metadata() schemamanager.SchemaMetadata {
	return schemamanager.SchemaMetadata{
		Name:        cm.schema.Metadata.Name,
		Catalog:     cm.schema.Metadata.Catalog,
		Variant:     cm.schema.Metadata.Variant,
		Path:        cm.schema.Metadata.Path,
		Description: cm.schema.Metadata.Description,
	}
}

func (cm *collectionManager) CollectionSchemaManager() schemamanager.CollectionSchemaManager {
	return cm.csm
}

func (cm *collectionManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: cm.schema.Version,
		Type:    types.CatalogObjectTypeCatalogCollection,
	}
	s.Values, _ = json.Marshal(cm.schema.Values)
	s.Schema, _ = json.Marshal(cm.schema.Spec)
	s.Description = cm.schema.Metadata.Description
	return &s
}

func (cm *collectionManager) SetCollectionSchemaManager(csm schemamanager.CollectionSchemaManager) {
	cm.csm = csm
}

func (cm *collectionManager) Values() schemamanager.ParamValues {
	return cm.schema.Values
}

func (cm *collectionManager) SetDefaultValues() apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}
	// set default values for the collection as defined in the schema
	cm.schema.Values = cm.csm.GetDefaultValues()
	return nil
}

func (cm *collectionManager) GetValue(ctx context.Context, param string) (types.NullableAny, apperrors.Error) {
	if v, ok := cm.schema.Values[param]; ok {
		return v.Value, nil
	}
	return types.NilAny(), ErrInvalidParameter.Msg("invalid parameter: " + param)
}

func (cm *collectionManager) SetValue(ctx context.Context, schemaLoaders schemamanager.SchemaLoaders, param string, value types.NullableAny) apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}
	if err := cm.csm.ValidateValue(ctx, schemaLoaders, param, value); err != nil {
		return err
	}
	// We need to copy the dataType and other annotations from the schema before we can copy over the value
	if cm.schema.Values == nil {
		cm.schema.Values = make(schemamanager.ParamValues)
	}
	v := cm.csm.GetValue(ctx, param)
	v.Value = value
	cm.schema.Values[param] = v
	return nil
}

func (cm *collectionManager) ValidateValues(ctx context.Context, schemaLoaders schemamanager.SchemaLoaders, currentValues schemamanager.ParamValues) apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}

	// There are few things to unwrap here:
	// At this time, the schema has all the parameters set in its Values. And these values either have the default set or are nil. But
	// the dataTypes and other annotations are always set.  So we need to copy all these over to the collection and substitute with new
	// values if the collection had any new values defined. Or we will copy over the defaults. If no defaults are set, the param will be a NullableAny
	// with dataType and other annotations set.
	if cm.schema.Values == nil {
		cm.schema.Values = make(schemamanager.ParamValues)
	}
	for _, param := range cm.csm.ParameterNames() {
		var currentValue schemamanager.ParamValue
		if v, ok := currentValues[param]; ok {
			currentValue = v
		}
		if v, ok := cm.schema.Spec.Values[param]; ok {
			if currentValue.Value.Equals(v) {
				cm.schema.Values[param] = currentValue
				continue
			}
			// if the user set any new value, we'll validate it and set it. If validation fails, we will return an error.
			if err := cm.SetValue(ctx, schemaLoaders, param, v); err != nil {
				return err
			}
		} else if !currentValue.Value.IsNil() {
			cm.schema.Values[param] = currentValue
		} else {
			// the values in the schema are already either the default or nil. But the dataType and other annotations are set. So it is safe to just copy over.
			cm.schema.Values[param] = cm.csm.GetValue(ctx, param)
		}
	}
	return nil
}
