package parameter

import (
	"testing"

	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/schema/errors"
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
				{Field: "Name", ErrStr: "missing required attribute"},
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
				{Field: "Name", ErrStr: "invalid name format \"Invalid Name!\"; allowed characters: [A-Za-z0-9_-]"},
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
				{Field: "Path", ErrStr: "invalid resource path; must start with '/' and contain only alphanumeric characters, underscores, and hyphens"},
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
				{Field: "DataType", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "invalid data type",
			input: ParameterSpec{
				DataType: "InvalidType",
			},
			expected: schemaerr.ValidationErrors{
				{Field: "DataType", ErrStr: "validation failed for attribute"},
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
