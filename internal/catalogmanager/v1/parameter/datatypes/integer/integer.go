package integer

import (
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager/datatyperegistry"
)

const (
	dataType = "Integer"
	version  = "v1"
)

type Validation struct {
	MinValue *int `json:"minValue" validate:"omitnil"`
	MaxValue *int `json:"maxValue" validate:"omitnil,integerBoundsValidator"`
	Step     *int `json:"step" validate:"omitnil,stepValidator"`
}

type Spec struct {
	DataType   string      `json:"dataType" validate:"required,eq=Integer"`
	Validation *Validation `json:"validation,omitempty" validate:"omitnil"`
	Default    *int        `json:"default,omitempty" validate:"omitnil"`
}

var _ schemamanager.Parameter = &Spec{}         // Ensure Spec implements schemamanager.Parameter
var _ datatyperegistry.Loader = LoadIntegerSpec // Ensure LoadIntegerSpec is a valid Loader

func (is *Spec) ValidateSpec() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(is)
	if err == nil {
		// validate the default value
		if is.Validation != nil && is.Default != nil {
			err := is.ValidateValue(*is.Default)
			if err != nil {
				return append(ves, schemaerr.ValidationError{
					Field:  "default",
					ErrStr: err.Error(),
				})
			}
		}
		return nil
	}

	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		ves = append(ves, schemaerr.ErrInvalidFieldSchema(""))
		return ves
	}

	value := reflect.ValueOf(is).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "stepValidator":
			ves = append(ves, schemaerr.ErrInvalidStepValue(jsonFieldName))
		case "integerBoundsValidator":
			ves = append(ves, schemaerr.ErrMaxValueLessThanMinValue(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}

	return ves
}

func (is *Spec) DefaultValue() any {
	if is.Default != nil {
		return *is.Default
	}
	return 0
}

func LoadIntegerSpec(data []byte) (schemamanager.Parameter, apperrors.Error) {
	is := &Spec{}
	err := json.Unmarshal(data, is)
	if err != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg("failed to read integer schema")
	}
	return is, nil
}

func init() {
	schemavalidator.V().RegisterValidation("stepValidator", integerStepValidator)
	schemavalidator.V().RegisterValidation("integerBoundsValidator", integerBoundsValidator)

	datatyperegistry.RegisterDataType(datatyperegistry.DataTypeKey{
		Type:    dataType,
		Version: version,
	}, LoadIntegerSpec)
}
