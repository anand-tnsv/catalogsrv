package catalogmanager

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestCollection(t *testing.T) {

	parameterYaml := `
		version: v1
		kind: ParameterSchema
		metadata:
			name: integer-param-schema
			catalog: example-catalog
		spec:
			dataType: Integer
			validation:
			minValue: 1
			maxValue: 10
			default: 5		
	`
	collectionYaml := `
		version: v1
		kind: CollectionSchema
		metadata:
			name: example-collection-schema
			catalog: example-catalog
			description: An example collection schema
		spec:
			parameters:
				maxRetries:
					schema: integer-param-schema
					default: 8
				maxDelay:
					dataType: Integer
					default: 1000
	`
	anotherCollectionYaml := `
		version: v1
		kind: CollectionSchema
		metadata:
			name: another-collection-schema
			catalog: example-catalog
			description: An example collection schema
		spec:
			parameters:
				maxRetries:
					schema: integer-param-schema
					default: 8
				maxDelay:
					dataType: Integer
					default: 1000
	`
	invalidCollectionvalueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			invalid: invalid
	`
	validCollectionValueYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: example-collection-schema
	`
	invalidCollectionValueYaml2 := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: invalid-schema
	`
	collectionWithChangedSchemaYaml := `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			catalog: example-catalog
			description: An example collection
			path: /some/random/path
		spec:
			schema: another-collection-schema
	`
	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})
	replaceTabsWithSpaces(&parameterYaml)
	replaceTabsWithSpaces(&collectionYaml)
	replaceTabsWithSpaces(&invalidCollectionvalueYaml)
	replaceTabsWithSpaces(&validCollectionValueYaml)
	replaceTabsWithSpaces(&invalidCollectionValueYaml2)
	replaceTabsWithSpaces(&collectionWithChangedSchemaYaml)
	replaceTabsWithSpaces(&anotherCollectionYaml)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")
	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)

	// create catalog example-catalog
	cat := &models.Catalog{
		Name:        "example-catalog",
		Description: "An example catalog",
		Info:        pgtype.JSONB{Status: pgtype.Null},
		ProjectID:   projectID,
	}
	err = db.DB(ctx).CreateCatalog(ctx, cat)
	assert.NoError(t, err)

	varId, err := db.DB(ctx).GetVariantIDFromName(ctx, cat.CatalogID, types.DefaultVariant)
	assert.NoError(t, err)

	// create a workspace
	ws := &models.Workspace{
		Info:        pgtype.JSONB{Status: pgtype.Null},
		BaseVersion: 1,
		VariantID:   varId,
		CatalogID:   cat.CatalogID,
	}
	err = db.DB(ctx).CreateWorkspace(ctx, ws)
	assert.NoError(t, err)

	// create the parameter schema
	jsonData, err := yaml.YAMLToJSON([]byte(parameterYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(collectionYaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection
	jsonData, err = yaml.YAMLToJSON([]byte(invalidCollectionvalueYaml))
	require.NoError(t, err)
	collection, err := NewCollectionManager(ctx, jsonData, nil)
	require.Error(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the collection
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection with invalid schema
	jsonData, err = yaml.YAMLToJSON([]byte(invalidCollectionValueYaml2))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the valid collection again
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionValueYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID), WithErrorIfEqualToExisting())
	require.ErrorIs(t, err, ErrEqualToExistingObject)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID), WithErrorIfExists())
	require.ErrorIs(t, err, ErrAlreadyExists)

	// create another collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(anotherCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection with changed schema
	jsonData, err = yaml.YAMLToJSON([]byte(collectionWithChangedSchemaYaml))
	require.NoError(t, err)
	collection, err = NewCollectionManager(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveCollection(ctx, collection, WithWorkspaceID(ws.WorkspaceID))
	require.ErrorIs(t, err, ErrSchemaOfCollectionNotMutable)
}
