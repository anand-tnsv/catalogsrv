package catalogmanager

import (
	"errors"
	"testing"

	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestNewCatalogManager(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		expected error
	}{
		{
			name: "valid catalog",
			jsonData: `
{
    "version": "v1",
    "kind": "Catalog",
    "metadata": {
        "name": "ValidCatalog",
        "description": "This is a valid catalog"
    }
}`,
			expected: nil,
		},
		{
			name: "invalid version",
			jsonData: `
{
    "version": "v2",
    "kind": "Catalog",
    "metadata": {
        "name": "InvalidVersionCatalog",
        "description": "Invalid version in catalog"
    }
}`,
			expected: ErrInvalidSchema,
		},
		{
			name: "invalid kind",
			jsonData: `
{
    "version": "v1",
    "kind": "InvalidKind",
    "metadata": {
        "name": "InvalidKindCatalog",
        "description": "Invalid kind in catalog"
    }
}`,
			expected: ErrInvalidSchema,
		},
		{
			name:     "empty JSON data",
			jsonData: "",
			expected: ErrInvalidSchema,
		},
	}

	// Initialize context with logger and database connection
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PDEFGH")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteTenant(ctx, tenantID)

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {

			// Convert JSON to []byte
			jsonData := []byte(tt.jsonData)

			// Create a new catalog manager
			cm, err := NewCatalogManager(ctx, jsonData, "CatalogName")

			// Check if the error string matches the expected error string
			if !errors.Is(tt.expected, err) {
				t.Errorf("got error %v, expected error %v", err, tt.expected)
			} else if tt.expected == nil {
				// If no error is expected, validate catalog properties
				assert.NotNil(t, cm)
				assert.Equal(t, "ValidCatalog", cm.Name())
				assert.Equal(t, "This is a valid catalog", cm.Description())

				// Save the catalog
				err = cm.Save(ctx)
				assert.NoError(t, err)

				// Attempt to save again to check for duplicate handling
				err = cm.Save(ctx)
				assert.Error(t, err)
				assert.ErrorIs(t, err, ErrAlreadyExists)

				// Load the catalog
				loadedCatalog, loadErr := LoadCatalogManagerByName(ctx, "ValidCatalog")
				assert.NoError(t, loadErr)
				assert.Equal(t, cm.Name(), loadedCatalog.Name())
				assert.Equal(t, cm.Description(), loadedCatalog.Description())

				// Load the catalog with an invalid name
				_, loadErr = LoadCatalogManagerByName(ctx, "InvalidCatalog")
				assert.Error(t, loadErr)
				assert.ErrorIs(t, loadErr, ErrCatalogNotFound)

				// Delete the catalog
				err = DeleteCatalogByName(ctx, "ValidCatalog")
				assert.NoError(t, err)

				// Try loading the deleted catalog
				_, loadErr = LoadCatalogManagerByName(ctx, "ValidCatalog")
				assert.Error(t, loadErr)

				// Try Deleting again
				err = DeleteCatalogByName(ctx, "ValidCatalog")
				assert.NoError(t, err) // should not return an error
			}
		})
	}
}
