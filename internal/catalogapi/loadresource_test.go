package catalogapi

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/errors"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/yaml"
)

func TestYamlToJson(t *testing.T) {
	y := `
version: v1
kind: Parameter
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`
	j, err := yaml.YAMLToJSON([]byte(y))
	if assert.NoError(t, err) {
		assert.NotEmpty(t, j)

		var prettyJSON []byte
		var err error
		// Indent the raw JSON
		buffer := bytes.NewBuffer(prettyJSON)
		err = json.Indent(buffer, j, "", "    ")
		if assert.NoError(t, err) {
			t.Logf("\n%s", buffer.String())
		}
	}
}

// Tests each section of a Parameter resource for validation errors
func TestNewParameterSchema(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		expected string
	}{
		{
			name: "valid resource",
			yamlData: `
version: v1
kind: Parameter
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: "",
		},
		{
			name: "missing required version",
			yamlData: `
kind: Parameter
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: schemaerr.ErrMissingRequiredAttribute("version").Error(),
		},
		{
			name: "bad name format",
			yamlData: `
version: v1
kind: Parameter
metadata:
  name: Invalid Name!
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: schemaerr.ErrInvalidNameFormat("metadata.name", "Invalid Name!").Error(),
		},
		{
			name: "bad dataType",
			yamlData: `
version: v1
kind: Parameter
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: InvalidType
  validation:
    minValue: 1
    maxValue: 10
  default: 5
`,
			expected: schemaerr.ErrUnsupportedDataType("spec.dataType", "InvalidType").Error(),
		},
		{
			name: "bad default value",
			yamlData: `
version: v1
kind: Parameter
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: 10
  default: 11
`,
			expected: schemaerr.ValidationError{
				Field:  "default",
				ErrStr: apierrors.ErrValueAboveMax.Error(),
			}.Error(),
		},
		{
			name: "bad validation values",
			yamlData: `
version: v1
kind: Parameter
metadata:
  name: example
  catalog: example-catalog
  path: /example
spec:
  dataType: Integer
  validation:
    minValue: 1
    maxValue: -1
  default: 5
`,
			expected: schemaerr.ErrMaxValueLessThanMinValue("validation.maxValue").Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = log.Logger.WithContext(ctx)
			jsonData, err := yaml.YAMLToJSON([]byte(tt.yamlData))
			if assert.NoError(t, err) {
				_, err := NewResource(ctx, jsonData)
				errStr := ""
				if err != nil {
					errStr = err.Error()
				}
				if errStr != tt.expected {
					t.Errorf("got %v, want %v", err, tt.expected)
				}
			}
		})
	}
}
