package schemas

import (
	"testing"

	"github.com/go-playground/validator/v10"
)

func TestResourcePathValidator(t *testing.T) {
	validate := validator.New()
	validate.RegisterValidation("resourcepath", resourcePathValidator)

	tests := []struct {
		input   string
		isValid bool
	}{
		{input: "/valid_path/with-collections", isValid: true},
		{input: "/valid_collection", isValid: true},
		{input: "/invalid-path/with@chars", isValid: false},
		{input: "relative/path", isValid: false},
		{input: "/another_valid-collection/", isValid: true},
		{input: "/collection_with_underscore/anotherCollection", isValid: true},
		{input: "/invalid-collection//double-slash", isValid: true},
		{input: "/", isValid: true},
		{input: "", isValid: false},
	}

	for _, test := range tests {
		err := validate.Var(test.input, "resourcepath")
		if (err == nil) != test.isValid {
			t.Errorf("Expected %v for input '%s', but got %v", test.isValid, test.input, err == nil)
		}
	}
}
