package parameter

import (
	"encoding/json"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/schemavalidator"
)

type ParameterSchema struct {
	Metadata ParameterMetadata `json:"metadata" validate:"required"`
	Spec     ParameterSpec     `json:"spec" validate:"required"`
}

type ParameterMetadata struct {
	Name    string `json:"name" validate:"required,nameFormatValidator"`
	Catalog string `json:"catalog" validate:"required,nameFormatValidator"`
	Path    string `json:"path" validate:"resourcePathValidator"`
}

type ParameterSpec struct {
	DataType   string          `json:"dataType" validate:"required"`
	Validation json.RawMessage `json:"validation"`
	Default    json.RawMessage `json:"default"`
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
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(e.Field()))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(e.Field(), val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidResourcePath(e.Field()))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(e.Field()))
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
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(e.Field()))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(e.Field()))
		}
	}
	return ves
}

func (ps *ParameterSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	ves = append(ves, ps.Metadata.Validate()...)
	if len(ves) == 0 {
		ves = append(ves, ps.Spec.Validate()...)
	}
	return ves
}

func ReadParameterSchema(version string, metadata, spec []byte) (*ParameterSchema, apperrors.Error) {
	ps := ParameterSchema{}
	err := json.Unmarshal(metadata, &ps.Metadata)
	if err != nil {
		return nil, apierrors.ErrSchemaValidation.Msg("failed to read parameter metadata")
	}
	err = json.Unmarshal(spec, &ps.Spec)
	if err != nil {
		return nil, apierrors.ErrSchemaValidation.Msg("failed to read parameter spec")
	}
	return &ps, nil
}
