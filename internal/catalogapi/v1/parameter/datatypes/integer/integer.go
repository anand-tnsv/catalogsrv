package integer

import (
	"encoding/json"

	"github.com/go-playground/validator/v10"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/schema/schemavalidator"
)

type Validation struct {
	MinValue        int  `json:"minValue,omitempty" validate:""`
	MaxValue        int  `json:"maxValue,omitempty" validate:"integerBoundsValidator"`
	Step            int  `json:"step,omitempty" validate:"stepValidator"`
	minValuePresent bool `json:"-"`
	maxValuePresent bool `json:"-"`
	stepPresent     bool `json:"-"`
}

type Spec struct {
	DataType   string     `json:"dataType" validate:"required,eq=Integer"`
	Validation Validation `json:"validation,omitempty"`
	Default    int        `json:"default,omitempty"`
}

var _ json.Unmarshaler = &Validation{} // Ensure Validation implements json.Unmarshaler

func (iv *Validation) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if _, ok := raw["minValue"]; ok {
		iv.minValuePresent = true
		if err := json.Unmarshal(raw["minValue"], &iv.MinValue); err != nil {
			return err
		}
	}

	if _, ok := raw["maxValue"]; ok {
		iv.maxValuePresent = true
		if err := json.Unmarshal(raw["maxValue"], &iv.MaxValue); err != nil {
			return err
		}
	}

	if _, ok := raw["step"]; ok {
		iv.stepPresent = true
		if err := json.Unmarshal(raw["step"], &iv.Step); err != nil {
			return err
		}
	}

	return nil
}

func integerBoundsValidator(fl validator.FieldLevel) bool {
	// Retrieve the parent Validation struct
	iv, ok := fl.Parent().Interface().(Validation)
	if !ok {
		return false
	}

	// If minValue is present, maxValue must be greater than minValue
	if iv.minValuePresent && iv.maxValuePresent && iv.MinValue > iv.MaxValue {
		return false
	}

	// If all conditions pass, return true indicating bounds are valid
	return true
}

// integerStepValidator validates the Step field in Validation
func integerStepValidator(fl validator.FieldLevel) bool {
	// Retrieve the parent Validation struct
	iv, ok := fl.Parent().Interface().(Validation)
	if !ok {
		return false
	}

	// Step should be included only if minValue is present and Step is positive
	// If stepPresent is true and Step is greater than zero, minValue must be present
	if iv.stepPresent && iv.Step > 0 && !iv.minValuePresent {
		return false
	}

	// Step should be included only if maxValue is present if Step is negative
	// If stepPresent is true and Step is less than zero, maxValue must be present
	if iv.stepPresent && iv.Step < 0 && !iv.maxValuePresent {
		return false
	}

	// Step should not be zero if it is present
	if iv.stepPresent && iv.Step == 0 {
		return false
	}

	// If Step is positive, and both minValue and maxValue are present,
	// ensure that minValue + Step does not exceed maxValue
	if iv.stepPresent && iv.Step > 0 && iv.maxValuePresent && iv.MinValue+iv.Step > iv.MaxValue {
		return false
	}

	// If Step is negative, and both minValue and maxValue are present,
	// ensure that minValue + Step does not exceed maxValue
	if iv.stepPresent && iv.Step < 0 && iv.minValuePresent && iv.MaxValue+iv.Step < iv.MinValue {
		return false
	}

	// If all conditions pass, return true indicating Step is valid
	return true
}

func (is *Spec) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(is)
	if err == nil {
		return nil
	}

	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		ves = append(ves, schemaerr.ValidationError{
			Field:  "",
			ErrStr: "invalid schema structure",
		})
		return ves
	}

	for _, e := range ve {
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "missing required attribute",
			})
		case "stepValidator":
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "step value is invalid: must be non-zero and valid, and if positive, minValue must be present; if negative, maxValue must be present",
			})
		case "integerBoundsValidator":
			ves = append(ves, schemaerr.ValidationError{
				Field:  e.Field(),
				ErrStr: "maxValue must be greater than minValue",
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
	schemavalidator.V().RegisterValidation("stepValidator", integerStepValidator)
	schemavalidator.V().RegisterValidation("integerBoundsValidator", integerBoundsValidator)
}
