package catalogmanager

import (
	"context"
	"testing"

	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/yaml"
)

func TestSaveObject(t *testing.T) {
	tests := []struct {
		name     string
		metadata schemamanager.ObjectMetadata
		yamlData string
		expected string
	}{
		{
			name: "valid parameter",
			metadata: schemamanager.ObjectMetadata{
				Name:    "example",
				Catalog: "example-catalog",
				Path:    "/example",
			},
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
			name: "valid collection with schema",
			metadata: schemamanager.ObjectMetadata{
				Name:    "AppConfigCollection",
				Catalog: "myCatalog",
				Path:    "/valid/path",
			},
			yamlData: `
version: v1
kind: Collection
metadata:
  name: AppConfigCollection
  catalog: myCatalog
  path: /valid/path
spec:
  parameters:
    maxRetries:
      schema: IntegerParamSchema
      default: 5
  collections:
    databaseConfig:
      schema: DatabaseConfigCollection
`,
			expected: "",
		},
	}
	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteTenant(ctx, tenantID)

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			jsonData, err := yaml.YAMLToJSON([]byte(tt.yamlData))
			if assert.NoError(t, err) {
				r, err := NewObject(ctx, jsonData, nil)
				errStr := ""
				if err != nil {
					errStr = err.Error()
				}
				if errStr != tt.expected {
					t.Errorf("got %v, want %v", err, tt.expected)
				} else {
					// Save the resource
					err = SaveObject(ctx, r.StorageRepresentation())
					if assert.NoError(t, err) {
						// try to save again
						err = SaveObject(ctx, r.StorageRepresentation(), true)
						if assert.Error(t, err) {
							assert.ErrorIs(t, err, ErrAlreadyExists)
						}
						// load the resource from the database
						lr, err := LoadObject(ctx, r.StorageRepresentation().GetHash(), &tt.metadata)
						if assert.NoError(t, err) { // Check if no error occurred
							assert.NotNil(t, lr)                                                                       // Check if the loaded resource is not nil
							assert.Equal(t, r.Kind(), lr.Kind())                                                       // Check if the kind matches
							assert.Equal(t, r.Version(), lr.Version())                                                 // Check if the version matches
							assert.Equal(t, r.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash()) // Check if the hashes match
						}
					}
				}
			}
		})
	}
}

func newDb() context.Context {
	ctx := log.Logger.WithContext(context.Background())
	ctx = db.ConnCtx(ctx)
	return ctx
}
