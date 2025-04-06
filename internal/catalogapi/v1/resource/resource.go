package resource

import (
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/schemavalidator"
	_ "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/v1/customvalidators" // Register custom validators
)

/*
type ResourceHeader struct {
	Version string `json:"version" validate:"required"`
	Kind    string `json:"kind" validate:"required,kindValidator"`
}
*/
// ResourceSchema represents a schema for a resource.
type ResourceSchema struct {
	Version  string          `json:"version" validate:"required"`
	Kind     string          `json:"kind" validate:"required,kindValidator"`
	Metadata json.RawMessage `json:"metadata" validate:"required"`
	Spec     json.RawMessage `json:"spec" validate:"required"`
}

const (
	ParameterKind  = "Parameter"
	CollectionKind = "Collection"
)

var validKinds = []string{
	ParameterKind,
	CollectionKind,
}

// kindValidator checks if the given kind is a valid resource kind.
func kindValidator(fl validator.FieldLevel) bool {
	kind := fl.Field().String()
	for _, validKind := range validKinds {
		if kind == validKind {
			return true
		}
	}
	return false
}

func (rs *ResourceSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(rs)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(rs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "kindValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrUnsupportedKind(jsonFieldName, val))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

// Read a json string in to a ResourceSchema struct
func ReadResourceSchema(jsonStr string) (*ResourceSchema, apperrors.Error) {
	rs := &ResourceSchema{}
	err := json.Unmarshal([]byte(jsonStr), rs)
	if err != nil {
		return nil, apierrors.ErrSchemaValidation.Msg("failed to read resource schema")
	}
	return rs, nil
}

func init() {
	schemavalidator.V().RegisterValidation("kindValidator", kindValidator)
}

const ResourceSchemaJsonSchema = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"version": {
				"type": "string"
			},
			"kind": {
				"type": "string",
			},
			"metadata": {
				"type": "object"
			},
			"spec": {
				"type": "object"
			}
		},
		"required": ["version", "kind", "metadata", "spec"],
		"additionalProperties": false
	}`
