package schemas

import (
	"github.com/go-playground/validator/v10"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/pkg/schemas/errors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/schemas/schemavalidator"
)

type ParameterSchema struct {
	ResourceHeader
	Metadata ParameterMetadata `json:"metadata" validate:"required"`
	Spec     ParameterSpec     `json:"spec" validate:"required"`
}

type ParameterMetadata struct {
	Name    string `json:"name" validate:"required,nameFormatValidator"`
	Catalog string `json:"catalog" validate:"required,nameFormatValidator"`
	Path    string `json:"path" validate:"resourcePathValidator"`
}

type ParameterSpec struct {
	DataType   string `json:"dataType" validate:"required,dataTypeValidator"`
	Validation any    `json:"validation"`
}

var validDataTypes = []string{
	"Integer",
	"String",
	"Boolean",
	"Float",
	"Array",
	"Dictionary",
	"Object",
}

func dataTypeValidator(fl validator.FieldLevel) bool {
	dataType := fl.Field().String()
	for _, validType := range validDataTypes {
		if dataType == validType {
			return true
		}
	}
	return false
}

func (ps *ParameterMetadata) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(ps)
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
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "invalid name format " + schemaerr.InQuotes(val) + "; allowed characters: [A-Za-z0-9_-]",
			})
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "invalid resource path; must start with '/' and contain only alphanumeric characters, underscores, and hyphens",
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

func (ps *ParameterSpec) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(ps)
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
		default:
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "validation failed for attribute",
			})
		}
	}
	return ves
}

func (ps *ParameterSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	ves = append(ves, ps.ResourceHeader.Validate()...)
	return ves
}

func init() {
	schemavalidator.V().RegisterValidation("dataTypeValidator", dataTypeValidator)
}
