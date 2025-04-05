package schemas

import (
	"github.com/go-playground/validator/v10"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/pkg/schemas/errors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/schemas/schemavalidator"
)

type ResourceHeader struct {
	Version string `json:"version" validate:"required"`
	Kind    string `json:"kind" validate:"required,kindValidator"`
}

// ResourceSchema represents a schema for a resource.
type ResourceSchema struct {
	ResourceHeader
	Metadata any `json:"metadata" validate:"required"`
	Spec     any `json:"spec" validate:"required"`
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

func (rh *ResourceHeader) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(rh)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}
	for _, e := range ve {
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "missing required attribute",
			})
		case "kindValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "invalid kind " + schemaerr.InQuotes(val),
			})
		default:
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "validation failed for attribute",
			})
		}
	}
	return ves
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
	for _, e := range ve {
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "missing required attribute",
			})
		case "kindValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "invalid kind " + schemaerr.InQuotes(val),
			})
		default:
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "validation failed for attribute",
			})
		}
	}
	return ves
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
