package collection

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager/datatyperegistry"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/parameter"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type CollectionSchema struct {
	Version string         `json:"version" validate:"required"`
	Spec    CollectionSpec `json:"spec" validate:"required"`
}

type CollectionSpec struct {
	Parameters map[string]Parameter `json:"parameters" validate:"omitempty,dive,keys,nameFormatValidator,endkeys,required"`
	//Collections map[string]Collection `json:"collections" validate:"omitempty,dive,keys,nameFormatValidator,endkeys,required"` // We don't maintain collection hierarcy here
}

type Parameter struct {
	Schema      string            `json:"schema" validate:"required_without=DataType,omitempty,nameFormatValidator"`
	DataType    string            `json:"dataType" validate:"required_without=Schema,excluded_unless=Schema '',omitempty,nameFormatValidator"`
	Default     types.NullableAny `json:"default"`
	Annotations Annotations       `json:"annotations" validate:"omitempty,dive,keys,noSpaces,endkeys,noSpaces"`
	Value       types.NullableAny `json:"value"`
}

type Annotations map[string]string

type Collection struct {
	Schema string `json:"schema" validate:"required,nameFormatValidator"`
}

func (cs *CollectionSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	// Note: We don't validate the dataType and default fields here
	// TODO: Add validation for dataType and default fields
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
		case "required_without":
			ves = append(ves, schemaerr.ErrMissingSchemaOrType(jsonFieldName))
		case "excluded_unless":
			ves = append(ves, schemaerr.ErrShouldContainSchemaOrType(jsonFieldName))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		case "noSpaces":
			ves = append(ves, schemaerr.ErrInvalidAnnotation(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

func (cs *CollectionSchema) ValidateDependencies(ctx context.Context, loaders schemamanager.ObjectLoaders) (schemamanager.ParameterReferences, schemaerr.ValidationErrors) {
	var ves schemaerr.ValidationErrors
	var refs schemamanager.ParameterReferences
	if loaders.ClosestParent == nil || loaders.ByHash == nil {
		return nil, append(ves, schemaerr.ErrMissingObjectLoaders(""))
	}
	for n, p := range cs.Spec.Parameters {
		if p.Schema != "" {
			var ve schemaerr.ValidationErrors
			refs, ve = validateParameterSchemaDependency(ctx, loaders, n, &p)
			ves = append(ves, ve...)
		} else if p.DataType != "" {
			ves = append(ves, validateDataTypeDependency(n, &p, cs.Version)...)
		}
	}
	return refs, ves
}

func (cs *CollectionSchema) ValidateValue(ctx context.Context, loaders schemamanager.ObjectLoaders, param string, value types.NullableAny) schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	if value.IsNil() {
		return ves
	}
	if loaders.ClosestParent == nil || loaders.ByHash == nil {
		return append(ves, schemaerr.ErrMissingObjectLoaders(""))
	}
	p, ok := cs.Spec.Parameters[param]
	if !ok {
		return append(ves, schemaerr.ErrInvalidParameter(param))
	}
	// shallow copy the parameter
	pShallowCopy := p
	pShallowCopy.Default = value
	if p.Schema != "" {
		_, ve := validateParameterSchemaDependency(ctx, loaders, param, &pShallowCopy)
		ves = append(ves, ve...)
	} else if p.DataType != "" {
		ves = append(ves, validateDataTypeDependency(param, &pShallowCopy, cs.Version)...)
	}
	return ves
}

func (cs *CollectionSchema) SetValue(ctx context.Context, param string, value types.NullableAny) error {
	p, ok := cs.Spec.Parameters[param]
	if !ok {
		return schemaerr.ErrInvalidParameter(param)
	}
	p.Value = value
	cs.Spec.Parameters[param] = p
	return nil
}

func (cs *CollectionSchema) SetDefaultValues(ctx context.Context) {
	for n, p := range cs.Spec.Parameters {
		if p.Default.IsNil() {
			continue
		}
		cs.SetValue(ctx, n, p.Default)
	}
}

func validateParameterSchemaDependency(ctx context.Context, loaders schemamanager.ObjectLoaders, name string, p *Parameter) (schemamanager.ParameterReferences, schemaerr.ValidationErrors) {
	var ves schemaerr.ValidationErrors
	var refs schemamanager.ParameterReferences
	// find if there is an applicable parameter schema.
	path, hash, err := loaders.ClosestParent(ctx, types.CatalogObjectTypeParameterSchema, p.Schema)
	if err != nil || path == "" || hash == "" {
		ves = append(ves, schemaerr.ErrParameterSchemaDoesNotExist(p.Schema))
	} else {
		refs = append(refs, schemamanager.ParameterReference{
			Parameter: path,
		})
		if !p.Default.IsNil() {
			om, err := loaders.ByHash(ctx, types.CatalogObjectTypeParameterSchema, hash, schemamanager.ObjectMetadata{
				Name: path[strings.LastIndex(path, "/")+1:],
				Path: path,
			})
			if err != nil && om == nil {
				ves = append(ves, schemaerr.ErrParameterSchemaDoesNotExist(p.Schema))
				return nil, ves
			}
			pm := om.ParameterManager()
			if pm == nil {
				ves = append(ves, schemaerr.ErrParameterSchemaDoesNotExist(p.Schema))
				return nil, ves
			}
			if err := pm.ValidateValue(p.Default); err != nil {
				ves = append(ves, schemaerr.ErrInvalidValue(name, err.Error()))
				return nil, ves
			}
		}
	}

	return refs, ves
}

func validateDataTypeDependency(name string, p *Parameter, version string) schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors

	//check if the DataType is supported
	loader := datatyperegistry.GetLoader(datatyperegistry.DataTypeKey{
		Type:    p.DataType,
		Version: version,
	})
	if loader == nil {
		ves = append(ves, schemaerr.ErrUnsupportedDataType("spec.parameters.dataType", p.DataType))
		return ves
	}
	if !p.Default.IsNil() {
		appendError := func() {
			ves = append(ves, schemaerr.ErrInvalidValue(name))
		}
		// construct a spec
		dataTypeSpec := parameter.ParameterSpec{
			DataType: p.DataType,
			Default:  p.Default,
		}
		js, err := json.Marshal(dataTypeSpec)
		if err != nil {
			appendError()
			return ves
		}
		parameter, apperr := loader(js)
		if apperr != nil {
			appendError()
			return ves
		}
		if err := parameter.ValidateValue(p.Default); err != nil {
			ves = append(ves, schemaerr.ErrInvalidValue(name, err.Error()))
			return ves
		}
	}
	return ves
}
