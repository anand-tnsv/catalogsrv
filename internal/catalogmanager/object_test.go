package catalogmanager

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgtype"
	_ "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestSaveObject(t *testing.T) {

	validParamYaml := `
				version: v1
				kind: Parameter
				metadata:
				  name: IntegerParamSchema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	validCollectionYaml := `
	version: v1
	kind: Collection
	metadata:
		name: AppConfigCollection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: IntegerParamSchema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`

	nonExistentParamYaml := `
	version: v1
	kind: Collection
	metadata:
		name: AppConfigCollection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: NonExistentParamSchema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`

	nonExistentDataTypeYaml := `
	version: v1
	kind: Collection
	metadata:
		name: AppConfigCollection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: NonExistentParamSchema
				default: 8
			maxDelay:
				dataType: InvalidType
				default: 1000
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&nonExistentParamYaml)
	replaceTabsWithSpaces(&nonExistentDataTypeYaml)

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

	// Create the parameter
	jsonData, err := yaml.YAMLToJSON([]byte(validParamYaml))
	if assert.NoError(t, err) {
		r, err := NewObject(ctx, jsonData, nil)
		if assert.NoError(t, err) {
			err = SaveObject(ctx, r, WithWorkspaceID(ws.WorkspaceID))
			if assert.NoError(t, err) {
				// try to save again
				err = SaveObject(ctx, r, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
				if assert.Error(t, err) {
					assert.ErrorIs(t, err, ErrAlreadyExists)
				}
				// create another object with same spec but at different path. Should not create a duplicate hash
				rNew, err := NewObject(ctx, jsonData, &schemamanager.ObjectMetadata{
					Name: "example_new",
					Path: "/another/path",
				})
				if assert.NoError(t, err) {
					err = SaveObject(ctx, rNew, WithWorkspaceID(ws.WorkspaceID))
					if assert.NoError(t, err) {
						assert.Equal(t, r.StorageRepresentation().GetHash(), rNew.StorageRepresentation().GetHash())
					}
				}
				// load the resource from the database
				m := r.Metadata()
				lr, err := LoadObjectByHash(ctx, r.StorageRepresentation().GetHash(), &m)
				if assert.NoError(t, err) { // Check if no error occurred
					assert.NotNil(t, lr)                                                                       // Check if the loaded resource is not nil
					assert.Equal(t, r.Kind(), lr.Kind())                                                       // Check if the kind matches
					assert.Equal(t, r.Version(), lr.Version())                                                 // Check if the version matches
					assert.Equal(t, r.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash()) // Check if the hashes match
				}
				// load object by path
				var tp types.CatalogObjectType
				if r.Kind() == "Collection" {
					tp = types.CatalogObjectTypeCollectionSchema
				} else if r.Kind() == "Parameter" {
					tp = types.CatalogObjectTypeParameterSchema
				}
				lr, err = LoadObjectByPath(ctx, tp, &m, WithWorkspaceID(ws.WorkspaceID))
				if assert.NoError(t, err) {
					assert.NotNil(t, lr)
					assert.Equal(t, r.Kind(), lr.Kind())
					assert.Equal(t, r.Version(), lr.Version())
					assert.Equal(t, r.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
				}
			}
		}
	}
	// Create the collection
	// unmarshal the yaml of the param schema
	param := make(map[string]any)
	yaml.Unmarshal([]byte(validParamYaml), &param)
	collection := make(map[string]any)
	yaml.Unmarshal([]byte(validCollectionYaml), &collection)
	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err := NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.NoError(t, err) {
			// load the collection schema
			m := collectionSchema.Metadata()
			lr, err := LoadObjectByHash(ctx, collectionSchema.StorageRepresentation().GetHash(), &m)
			if assert.NoError(t, err) {
				assert.NotNil(t, lr)
				assert.Equal(t, collectionSchema.Kind(), lr.Kind())
				assert.Equal(t, collectionSchema.Version(), lr.Version())
				assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
			}
			// load by path
			lr, err = LoadObjectByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
			if assert.NoError(t, err) {
				assert.NotNil(t, lr)
				assert.Equal(t, collectionSchema.Kind(), lr.Kind())
				assert.Equal(t, collectionSchema.Version(), lr.Version())
				assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
			}
		}
	}
	// change the base path of the collection schema
	collectionSchema.SetPath("/another/collection/path")
	err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}

	// revert the path
	collectionSchema.SetPath("/valid/path")
	// change default value to a string
	collection["spec"].(map[string]any)["parameters"].(map[string]any)["maxRetries"].(map[string]any)["default"] = "five"
	jsonData, err = json.Marshal(collection)
	require.NoError(t, err)
	collectionSchema, err = NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with a non-existent parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(nonExistentParamYaml))
	require.NoError(t, err)
	collectionSchema, err = NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with a non-existent data type
	jsonData, err = yaml.YAMLToJSON([]byte(nonExistentDataTypeYaml))
	require.NoError(t, err)
	collectionSchema, err = NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}
}

func TestSaveValue(t *testing.T) {

	validParamYaml := `
				version: v1
				kind: Parameter
				metadata:
				  name: IntegerParamSchema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	validCollectionYaml := `
	version: v1
	kind: Collection
	metadata:
		name: AppConfigCollection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: IntegerParamSchema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validValueYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /valid/path/AppConfigCollection
	spec:
		maxRetries: 5
		maxDelay: 2000
	`

	invalidDataTypeYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /valid/path/AppConfigCollection
	spec:
		maxRetries: 5
		maxDelay: two_thousand
	`
	invalidParamYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /valid/path/AppConfigCollection
	spec:
		maxRetries: 5000
		maxDelay: 2000
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&validValueYaml)
	replaceTabsWithSpaces(&invalidDataTypeYaml)
	replaceTabsWithSpaces(&invalidParamYaml)

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

	// Create the parameter
	jsonData, err := yaml.YAMLToJSON([]byte(validParamYaml))
	if assert.NoError(t, err) {
		r, err := NewObject(ctx, jsonData, nil)
		require.NoError(t, err)
		err = SaveObject(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	// Create the collection
	// unmarshal the yaml of the param schema
	param := make(map[string]any)
	yaml.Unmarshal([]byte(validParamYaml), &param)
	collection := make(map[string]any)
	yaml.Unmarshal([]byte(validCollectionYaml), &collection)
	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err := NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}

	// create a value
	jsonData, err = yaml.YAMLToJSON([]byte(validValueYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	assert.NoError(t, err)

	// create a value with invalid data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDataTypeYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}

	// create a value with invalid parameter
	jsonData, err = yaml.YAMLToJSON([]byte(invalidParamYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}
}

func TestReferences(t *testing.T) {
	validParamYaml := `
				version: v1
				kind: Parameter
				metadata:
				  name: IntegerParamSchema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	updatedParamYaml := `
				version: v1
				kind: Parameter
				metadata:
				  name: IntegerParamSchema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 20
				  default: 5
	`
	validParamYaml2 := `
				version: v1
				kind: Parameter
				metadata:
				  name: IntegerParamSchema2
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	validCollectionYaml := `
	version: v1
	kind: Collection
	metadata:
		name: AppConfigCollection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: IntegerParamSchema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validCollectionYaml2 := `
	version: v1
	kind: Collection
	metadata:
		name: AppConfigCollection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			connectionAttempts:
				schema: IntegerParamSchema2
				default: 3
			connectionDelay:
				schema: IntegerParamSchema
				default: 7	
			maxRetries:
				schema: IntegerParamSchema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&updatedParamYaml)
	replaceTabsWithSpaces(&validParamYaml2)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&validCollectionYaml2)

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

	// get the directories for the workspace
	dir, err := getDirectoriesForWorkspace(ctx, ws.WorkspaceID)
	require.NoError(t, err)

	// Create the parameter
	jsonData, err := yaml.YAMLToJSON([]byte(validParamYaml))
	var paramFqn string
	if assert.NoError(t, err) {
		r, err := NewObject(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn = r.FullyQualifiedName()
		err = SaveObject(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	// Create the collection
	// unmarshal the yaml of the param schema
	param := make(map[string]any)
	yaml.Unmarshal([]byte(validParamYaml), &param)
	collection := make(map[string]any)
	yaml.Unmarshal([]byte(validCollectionYaml), &collection)
	// create the collection schema
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err := NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getObjectReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: paramFqn}})
		refs, err = getObjectReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: collectionSchema.FullyQualifiedName()}})
	}

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml2))
	var paramFqn2 string
	if assert.NoError(t, err) {
		r, err := NewObject(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn2 = r.FullyQualifiedName()
		err = SaveObject(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	// update the collection schema to include another parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml2))
	require.NoError(t, err)
	collectionSchema, err = NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getObjectReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 2)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: paramFqn2}, {Parameter: paramFqn}})
		refs, err = getObjectReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: collectionSchema.FullyQualifiedName()}})
		refs, err = getObjectReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: collectionSchema.FullyQualifiedName()}})
	}
	// update the collection back
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewObject(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveObject(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getObjectReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: paramFqn}})
		refs, err = getObjectReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: collectionSchema.FullyQualifiedName()}})
		refs, err = getObjectReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.Len(t, refs, 0)
	}
	// update the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamYaml))
	if assert.NoError(t, err) {
		r, err := NewObject(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn = r.FullyQualifiedName()
		err = SaveObject(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getObjectReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.ParameterReference{{Parameter: collectionSchema.FullyQualifiedName()}})
	}
}

func newDb() context.Context {
	ctx := log.Logger.WithContext(context.Background())
	ctx = db.ConnCtx(ctx)
	return ctx
}

func replaceTabsWithSpaces(s *string) {
	*s = strings.ReplaceAll(*s, "\t", "    ")
}
