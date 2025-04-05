package resource

import (
	"encoding/json"
	"testing"

	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/schema/schemavalidator"
	"github.com/stretchr/testify/assert"
)

func TestResourceSchema_Validate(t *testing.T) {
	tests := []struct {
		name     string
		input    ResourceSchema
		expected schemaerr.ValidationErrors
	}{
		{
			name: "valid resource schema",
			input: ResourceSchema{
				ResourceHeader: ResourceHeader{
					Version: "v1",
					Kind:    "Parameter",
				},
				Metadata: json.RawMessage(`{"name": "example"}`),
				Spec:     json.RawMessage(`{"description": "example spec"}`),
			},
			expected: nil,
		},
		{
			name: "missing required version",
			input: ResourceSchema{
				ResourceHeader: ResourceHeader{
					Kind: "Parameter",
				},
				Metadata: json.RawMessage(`{"name": "example"}`),
				Spec:     json.RawMessage(`{"description": "example spec"}`),
			},
			expected: schemaerr.ValidationErrors{
				{Field: "Version", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "invalid kind",
			input: ResourceSchema{
				ResourceHeader: ResourceHeader{
					Version: "v1",
					Kind:    "InvalidKind",
				},
				Metadata: json.RawMessage(`{"name": "example"}`),
				Spec:     json.RawMessage(`{"description": "example spec"}`),
			},
			expected: schemaerr.ValidationErrors{
				{Field: "Kind", ErrStr: "invalid kind \"InvalidKind\""},
			},
		},
		{
			name: "missing required metadata",
			input: ResourceSchema{
				ResourceHeader: ResourceHeader{
					Version: "v1",
					Kind:    "Parameter",
				},
				Spec: json.RawMessage(`{"description": "example spec"}`),
			},
			expected: schemaerr.ValidationErrors{
				{Field: "Metadata", ErrStr: "missing required attribute"},
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

func TestValidateJsonSchema(t *testing.T) {
	t.Skip("skipping test")
	tests := []struct {
		name     string
		input    string
		expected schemaerr.ValidationErrors
	}{
		{
			name: "valid resource schema",
			input: `{
				"version": "v1",
				"kind": "Parameter",
				"metadata": {"name": "example"},
				"spec": {"description": "example spec"}
			}`,
			expected: nil,
		},
		{
			name: "missing required version",
			input: `{
				"kind": "Parameter",
				"metadata": {"name": "example"},
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).version", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "invalid kind type",
			input: `{
				"version": "v1",
				"kind": 123,
				"metadata": {"name": "example"},
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).kind", ErrStr: "invalid type"},
			},
		},
		{
			name: "missing required metadata",
			input: `{
				"version": "v1",
				"kind": "Parameter",
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).metadata", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "missing required spec",
			input: `{
				"version": "v1",
				"kind": "Parameter",
				"metadata": {"name": "example"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).spec", ErrStr: "missing required attribute"},
			},
		},
		{
			name: "invalid metadata type",
			input: `{
				"version": "v1",
				"kind": "Parameter",
				"metadata": "this should be an object",
				"spec": {"description": "example spec"}
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).metadata", ErrStr: "invalid type"},
			},
		},
		{
			name: "invalid spec type",
			input: `{
				"version": "v1",
				"kind": "Parameter",
				"metadata": {"name": "example"},
				"spec": "this should be an object"
			}`,
			expected: schemaerr.ValidationErrors{
				{Field: "(root).spec", ErrStr: "invalid type"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := schemavalidator.ValidateJsonSchema(ResourceSchemaJsonSchema, tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
