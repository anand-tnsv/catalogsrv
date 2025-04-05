package datatypes

import (
	"encoding/json"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemas"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemas/schemavalidator"
)

type IntegerValidation struct {
	MinValue        int  `json:"minValue,omitempty" validate:""`
	MaxValue        int  `json:"maxValue,omitempty" validate:"gtfield=MinValue"`
	Step            int  `json:"step,omitempty" validate:"stepValidator"`
	minValuePresent bool `json:"-"`
	maxValuePresent bool `json:"-"`
	stepPresent     bool `json:"-"`
}

type IntegerSpec struct {
	Validation IntegerValidation `json:"validation,omitempty"`
	Default    int               `json:"default,omitempty"`
}

var _ json.Unmarshaler = &IntegerValidation{} // Ensure IntegerValidation implements json.Unmarshaler

func (iv *IntegerValidation) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if _, ok := raw["minValue"]; ok {
		iv.minValuePresent = true
		if err := json.Unmarshal(raw["minValue"], &iv.minValuePresent); err != nil {
			return err
		}
	}

	if _, ok := raw["maxValue"]; ok {
		iv.maxValuePresent = true
		if err := json.Unmarshal(raw["maxValue"], &iv.maxValuePresent); err != nil {
			return err
		}
	}

	if _, ok := raw["step"]; ok {
		iv.stepPresent = true
		if err := json.Unmarshal(raw["step"], &iv.stepPresent); err != nil {
			return err
		}
	}

	return nil
}

// integerStepValidator validates the Step field in IntegerValidation
func integerStepValidator(fl validator.FieldLevel) bool {
	// Retrieve the parent IntegerValidation struct
	iv, ok := fl.Parent().Interface().(IntegerValidation)
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

	// If all conditions pass, return true indicating Step is valid
	return true
}

func (is *IntegerSpec) Validate() schemas.ValidationErrors {
	var ves schemas.ValidationErrors
	err := schemavalidator.V().Struct(is)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemas.ErrInvalidSchema)
	}
	for _, e := range ve {
		switch e.Tag() {
		case "required":
			ves = append(ves, schemas.ValidationError{
				Field:  e.Field(),
				ErrStr: "missing required attribute",
			})
		case "stepValidator":
			ves = append(ves, schemas.ValidationError{
				Field:  e.Field(),
				ErrStr: "invalid step value",
			})
		default:
			ves = append(ves, schemas.ValidationError{
				Field:  e.Field(),
				ErrStr: "validation failed for attribute",
			})
		}
	}
	return ves
}

func init() {
	schemavalidator.V().RegisterValidation("stepValidator", integerStepValidator)
}
