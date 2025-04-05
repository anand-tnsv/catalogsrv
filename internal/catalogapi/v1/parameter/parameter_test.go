package parameter

import (
	"testing"

	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/errors"
	"github.com/stretchr/testify/assert"
)

func TestParameterMetadata_Validate(t *testing.T) {
	tests := []struct {
		name     string
		input    ParameterMetadata
		expected schemaerr.ValidationErrors
	}{
		{
			name: "valid parameter metadata",
			input: ParameterMetadata{
				Name:    "validName",
				Catalog: "validCatalog",
				Path:    "/valid_path",
			},
			expected: nil,
		},
		{
			name: "missing required name",
			input: ParameterMetadata{
				Catalog: "validCatalog",
				Path:    "/valid_path",
			},
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingRequiredAttribute("Name"),
			},
		},
		{
			name: "invalid name format",
			input: ParameterMetadata{
				Name:    "Invalid Name!",
				Catalog: "validCatalog",
				Path:    "/valid_path",
			},
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidNameFormat("Name", "Invalid Name!"),
			},
		},
		{
			name: "invalid resource path",
			input: ParameterMetadata{
				Name:    "validName",
				Catalog: "validCatalog",
				Path:    "invalid_path",
			},
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrInvalidResourcePath("Path"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.input.Validate()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestParameterSpec_Validate(t *testing.T) {
	tests := []struct {
		name     string
		input    ParameterSpec
		expected schemaerr.ValidationErrors
	}{
		{
			name: "valid parameter spec",
			input: ParameterSpec{
				DataType: "Integer",
			},
			expected: nil,
		},
		{
			name:  "missing required data type",
			input: ParameterSpec{},
			expected: schemaerr.ValidationErrors{
				schemaerr.ErrMissingRequiredAttribute("DataType"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.input.Validate()
			assert.Equal(t, tt.expected, actual)
		})
	}
}
