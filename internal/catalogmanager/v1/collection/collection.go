package collection

import (
	"reflect"

	"github.com/go-playground/validator/v10"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	_ "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/customvalidators"
)

type CollectionSchema struct {
	Version string         `json:"version" validate:"required"`
	Spec    CollectionSpec `json:"spec" validate:"required"`
}

type CollectionSpec struct {
	Parameters  map[string]Parameter  `json:"parameters" validate:"omitempty,dive,keys,nameFormatValidator,endkeys,required"`
	Collections map[string]Collection `json:"collections" validate:"omitempty,dive,keys,nameFormatValidator,endkeys,required"`
}

type Parameter struct {
	Schema      string      `json:"schema" validate:"required_without=DataType,omitempty,nameFormatValidator"`
	DataType    string      `json:"dataType" validate:"required_without=Schema,excluded_unless=Schema '',omitempty,nameFormatValidator"`
	Default     any         `json:"default" validate:"omitempty"`
	Annotations Annotations `json:"annotations" validate:"omitempty,dive,keys,noSpaces,endkeys,noSpaces"`
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
			ves = append(ves, schemaerr.ErrInvalidResourcePath(jsonFieldName))
		case "noSpaces":
			ves = append(ves, schemaerr.ErrInvalidAnnotation(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}
