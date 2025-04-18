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
	"github.com/mugiliam/hatchcatalogsrv/internal/config"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
	"sigs.k8s.io/yaml"
)

func TestSaveSchema(t *testing.T) {
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		description: An example collection
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validParamYaml := `
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
	validParamYamlModifiedValidation := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
	`
	invalidDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: InvalidInteger
				default: 1000
	`
	invalidDefaultYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 'hello'
			maxDelay:
				dataType: Integer
				default: 1000
	`
	invalidDefaultDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 5
			maxDelay:
				dataType: Integer
				default: 'hello'
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&invalidDataTypeYamlCollection)
	replaceTabsWithSpaces(&validParamYamlModifiedValidation)
	replaceTabsWithSpaces(&invalidDefaultYamlCollection)
	replaceTabsWithSpaces(&invalidDefaultDataTypeYamlCollection)

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

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the same collection again with error if equal
	err = SaveSchema(ctx, collectionSchema, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create a collection with no existing parameter schemas
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the same schema again
	err = SaveSchema(ctx, parameterSchema, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	err = SaveSchema(ctx, parameterSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the collection with the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// Load the collection schema
	m := collectionSchema.Metadata()
	lr, err := LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), collectionSchema.Kind())
	assert.Equal(t, collectionSchema.Version(), lr.Version())
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// Load the parameter schema
	m = parameterSchema.Metadata()
	lr, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), parameterSchema.Kind())
	assert.Equal(t, parameterSchema.Version(), lr.Version())
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// create a collection with invalid data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDataTypeYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create a collection with invalid default value
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDefaultYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create a collection with invalid default value data type
	jsonData, err = yaml.YAMLToJSON([]byte(invalidDefaultDataTypeYamlCollection))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// modify the parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// try to delete the parameter schema
	dir, err := getDirectoriesForWorkspace(ctx, ws.WorkspaceID)
	require.NoError(t, err)
	err = deleteParameterSchema(ctx, "/integer-param-schema", dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	// delete the collection
	err = deleteCollectionSchema(ctx, "/path", dir)
	require.NoError(t, err)
	// delete the parameter schema
	err = deleteParameterSchema(ctx, "/integer-param-schema", dir)
	require.NoError(t, err)
}

func TestSchemaWithNamespaces(t *testing.T) {
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		namespace: my-namespace
		description: An example collection
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		namespace: my-namespace
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validParamYaml := `
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
	validParamYamlModifiedValidation := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
	`
	invalidDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: InvalidInteger
				default: 1000
	`
	invalidDefaultYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 'hello'
			maxDelay:
				dataType: Integer
				default: 1000
	`
	invalidDefaultDataTypeYamlCollection := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: some-collection
		catalog: example-catalog
		description: An example collection
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 5
			maxDelay:
				dataType: Integer
				default: 'hello'
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&invalidDataTypeYamlCollection)
	replaceTabsWithSpaces(&validParamYamlModifiedValidation)
	replaceTabsWithSpaces(&invalidDefaultYamlCollection)
	replaceTabsWithSpaces(&invalidDefaultDataTypeYamlCollection)

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

	// create a namespace
	namespace := &models.Namespace{
		Name:        "my-namespace",
		CatalogID:   cat.CatalogID,
		VariantID:   varId,
		Description: "An example namespace for testing",
		Info:        nil,
	}
	err = db.DB(ctx).CreateNamespace(ctx, namespace)
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

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// retrieve the collection
	m := collectionSchema.Metadata()
	m.Path = ""
	lr, err := LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), collectionSchema.Kind())
	assert.Equal(t, collectionSchema.Version(), lr.Version())
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	// Verify the namespace is set correctly
	assert.Equal(t, lr.Metadata().Namespace.String(), "my-namespace", "Expected namespace to be 'my-namespace'")

	// create the same collection again
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create same collection in the root namespace
	b, err := sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithErrorIfExists(), WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the root collection is saved correctly
	m = collectionSchema.Metadata()
	m.Path = "" // clear path to load root collection
	lr, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())

	// create the same collection in a different namespace
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "default")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the new namespace collection is saved correctly
	m = collectionSchema.Metadata()
	m.Path = "" // clear path to load the new namespace collection
	lr, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	assert.Equal(t, lr.Metadata().Namespace.String(), "default", "Expected namespace to be 'another'")

	// create a collection with parameters now
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the parameter schema in root namespace
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the parameter schema is saved correctly in the root namespace
	m = parameterSchema.Metadata()
	m.Path = "" // clear path to load root parameter schema
	lr, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, lr.Kind(), parameterSchema.Kind())
	assert.Equal(t, parameterSchema.Version(), lr.Version())
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	// Verify the namespace is not set for root parameter schema
	assert.Equal(t, lr.Metadata().Namespace.String(), "", "Expected root parameter schema to have no namespace")

	// create the collection with the parameter in the namespace
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collection in the root namespace
	b, err = sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the parameter schema in the namespace with modified max value
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create it with a different name
	b, err = sjson.Set(string(jsonData), "metadata.name", "integer-param-schema-modified")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	// Verify the parameter schema is saved correctly in the namespace
	m = parameterSchema.Metadata()
	m.Path = "" // clear path to load the namespace parameter schema
	lr, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	assert.Equal(t, parameterSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
	assert.Equal(t, lr.Metadata().Namespace.String(), "my-namespace", "Expected namespace to be 'my-namespace'")

	// create the collections schema again
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// modify the parameter schema in the root namespace with modified max value when a collection is referring to it
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYamlModifiedValidation))
	require.NoError(t, err)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// create the same but ignore the conflict.  This will cause revalidation error
	err = SaveSchema(ctx, parameterSchema, IgnoreSchemaSpecChange(), WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)
	// skip revalidation
	err = SaveSchema(ctx, parameterSchema, IgnoreSchemaSpecChange(), SkipRevalidationOnSchemaChange(), WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create the collections schema again.  Should error out since the max value in the parameter schema has changed
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// modify the collection schema to reduce the maxRetry value and try again
	b, err = sjson.Set(string(jsonData), "spec.parameters.maxRetries.default", 5)
	require.NoError(t, err)
	jsonData = []byte(b)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// delete the parameter schema
	dir, err := getDirectoriesForWorkspace(ctx, ws.WorkspaceID)
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, "/my-namespace/integer-param-schema-modified", dir)
	require.NoError(t, err)
	// Verify the parameter schema is deleted
	m = parameterSchema.Metadata()
	m.Name = "integer-param-schema-modified" // set name to load the deleted collection schema
	m.Namespace = types.NullableStringFrom("my-namespace")
	m.Path = "" // clear path to load the deleted collection schema
	_, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err)

	// delete the parameter schema that has references
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, "/integer-param-schema", dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	// delete the collection schema
	err = deleteCollectionSchema(ctx, "/valid", dir)
	require.NoError(t, err)
	// Verify the collection schema is deleted
	m = collectionSchema.Metadata()
	m.Name = "valid" // set name to load the deleted collectio n schema
	m.Namespace = types.NullString()
	m.Path = "/" // clear path to load the deleted collection schema
	_, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err, "Expected error when loading deleted collection schema")
	err = deleteCollectionSchema(ctx, "/my-namespace/path", dir)
	require.NoError(t, err)
	// Verify the root collection schema is deleted
	m = collectionSchema.Metadata()
	m.Name = "path"                                        // set name to load the deleted collection schema
	m.Namespace = types.NullableStringFrom("my-namespace") // set namespace to load the deleted collection schema
	m.Path = "/"
	_, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.Error(t, err, "Expected error when loading deleted root collection schema")

	// delete the parameter schema that has references
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, "/integer-param-schema", dir)
	require.ErrorIs(t, err, ErrUnableToDeleteParameterWithReferences)

	err = deleteCollectionSchema(ctx, "/path", dir)
	require.NoError(t, err)
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, "/integer-param-schema", dir)
	require.NoError(t, err)

	// create two identical parameter schemas in differentnamespaces
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	require.NoError(t, err)
	b, err = sjson.Set(string(jsonData), "metadata.namespace", "my-namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// create another identical parameter schema in a different namespace
	b, err = sjson.Delete(string(jsonData), "metadata.namespace")
	require.NoError(t, err)
	jsonData = []byte(b)
	parameterSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// delete one
	err = DeleteSchema(ctx, types.CatalogObjectTypeParameterSchema, "/my-namespace/integer-param-schema", dir)
	require.NoError(t, err)
	// Verify the parameter schema in the other namespace is still available
	m = parameterSchema.Metadata()
	m.Name = "integer-param-schema"  // set name to load the deleted collection schema
	m.Namespace = types.NullString() // clear namespace to load the other parameter schema
	m.Path = ""                      // clear path to load the other parameter schema
	_, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeParameterSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
}
func TestSaveHierarchicalSchema(t *testing.T) {
	if !config.HierarchicalSchemas {
		t.Skip("Hierarchical schemas are not enabled")
	}
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		path: /
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /valid
	`
	emptyCollection3Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: another
		catalog: example-catalog
		path: /
	`
	emptyCollection4Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /another
	`
	emptyCollection5Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: collection
		catalog: example-catalog
		path: /another
	`
	emptyCollection6Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /another/collection
	`
	validParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
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
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`

	nonExistentParamYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: non-existent-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`

	nonExistentDataTypeYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: non-existent-param-schema
				default: 8
			maxDelay:
				dataType: InvalidType
				default: 1000
	`
	invalidParameterPath := `
	version: v1
	kind: ParameterSchema
	metadata:
		name: integer-param-schema
		catalog: example-catalog
		path: /invalid/path
	spec:
		dataType: Integer
		validation:
		minValue: 1
		maxValue: 10
		default: 5
	`
	invalidCollectionPath := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /invalid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
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

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&emptyCollection3Yaml)
	replaceTabsWithSpaces(&emptyCollection4Yaml)
	replaceTabsWithSpaces(&emptyCollection5Yaml)
	replaceTabsWithSpaces(&emptyCollection6Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&nonExistentParamYaml)
	replaceTabsWithSpaces(&nonExistentDataTypeYaml)
	replaceTabsWithSpaces(&invalidParameterPath)
	replaceTabsWithSpaces(&invalidCollectionPath)

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

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection3Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection4Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection5Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection6Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		if assert.NoError(t, err) {
			err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
			if assert.NoError(t, err) {
				// try to save again
				err = SaveSchema(ctx, r, WithErrorIfEqualToExisting(), WithWorkspaceID(ws.WorkspaceID))
				if assert.Error(t, err) {
					assert.ErrorIs(t, err, ErrAlreadyExists)
				}
				// create another object with same spec but at different path. Should not create a duplicate hash
				rNew, err := NewSchema(ctx, jsonData, &schemamanager.SchemaMetadata{
					Name: "example-new",
					Path: "/another/path",
				})
				if assert.NoError(t, err) {
					err = SaveSchema(ctx, rNew, WithWorkspaceID(ws.WorkspaceID))
					if assert.NoError(t, err) {
						assert.Equal(t, r.StorageRepresentation().GetHash(), rNew.StorageRepresentation().GetHash())
					}
				}
				// load the resource from the database
				m := r.Metadata()
				lr, err := LoadSchemaByHash(ctx, r.StorageRepresentation().GetHash(), &m)
				if assert.NoError(t, err) { // Check if no error occurred
					assert.NotNil(t, lr)                                                                       // Check if the loaded resource is not nil
					assert.Equal(t, r.Kind(), lr.Kind())                                                       // Check if the kind matches
					assert.Equal(t, r.Version(), lr.Version())                                                 // Check if the version matches
					assert.Equal(t, r.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash()) // Check if the hashes match
				}
				// load object by path
				var tp types.CatalogObjectType
				if r.Kind() == "CollectionSchema" {
					tp = types.CatalogObjectTypeCollectionSchema
				} else if r.Kind() == "ParameterSchema" {
					tp = types.CatalogObjectTypeParameterSchema
				}
				lr, err = LoadSchemaByPath(ctx, tp, &m, WithWorkspaceID(ws.WorkspaceID))
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
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.NoError(t, err) {
			// load the collection schema
			m := collectionSchema.Metadata()
			lr, err := LoadSchemaByHash(ctx, collectionSchema.StorageRepresentation().GetHash(), &m)
			if assert.NoError(t, err) {
				assert.NotNil(t, lr)
				assert.Equal(t, collectionSchema.Kind(), lr.Kind())
				assert.Equal(t, collectionSchema.Version(), lr.Version())
				assert.Equal(t, collectionSchema.StorageRepresentation().GetHash(), lr.StorageRepresentation().GetHash())
			}
			// load by path
			lr, err = LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
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
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}

	// revert the path
	collectionSchema.SetPath("/valid/path")
	// change default value to a string
	collection["spec"].(map[string]any)["parameters"].(map[string]any)["maxRetries"].(map[string]any)["default"] = "five"
	jsonData, err = json.Marshal(collection)
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with a non-existent parameter schema
	jsonData, err = yaml.YAMLToJSON([]byte(nonExistentParamYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with a non-existent data type
	jsonData, err = yaml.YAMLToJSON([]byte(nonExistentDataTypeYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a parameter with an invalid path
	jsonData, err = yaml.YAMLToJSON([]byte(invalidParameterPath))
	require.NoError(t, err)
	parameterSchema, err := NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, parameterSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}

	// create a collection with an invalid path
	jsonData, err = yaml.YAMLToJSON([]byte(invalidCollectionPath))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		if assert.Error(t, err) {
			t.Logf("Error: %v", err)
		}
	}
}

func TestSaveHierarchicalValue(t *testing.T) {
	if !config.HierarchicalSchemas {
		t.Skip("Hierarchical schemas are not enabled")
	}
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		path: /
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /valid
	`

	validParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
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
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
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
		collection: /valid/path/app-config-collection
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
		collection: /valid/path/app-config-collection
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
		collection: /valid/path/app-config-collection
	spec:
		maxRetries: 5000
		maxDelay: 2000
	`
	invalidPathYaml := `
	version: v1
	kind: Value
	metadata:
		catalog: example-catalog
		variant: default
		collection: /invalidpath/app-config-collection
	spec:
		maxRetries: 5
		maxDelay: 1000
	`

	// Run tests
	// Initialize context with logger and database connection
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&validValueYaml)
	replaceTabsWithSpaces(&invalidDataTypeYaml)
	replaceTabsWithSpaces(&invalidParamYaml)
	replaceTabsWithSpaces(&invalidPathYaml)

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

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
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
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	collectionHash := collectionSchema.StorageRepresentation().GetHash()

	// create a value
	jsonData, err = yaml.YAMLToJSON([]byte(validValueYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// get the value
	dir, err := getDirectoriesForWorkspace(ctx, ws.WorkspaceID)
	require.NoError(t, err)

	_, err = GetValue(ctx, &ValueMetadata{
		Catalog:    collectionSchema.Catalog(),
		Variant:    collectionSchema.Metadata().Variant,
		Collection: collectionSchema.FullyQualifiedName(),
	},
		dir)
	require.NoError(t, err)

	// load collection by path
	m := collectionSchema.Metadata()
	lr, err := LoadSchemaByPath(ctx, types.CatalogObjectTypeCollectionSchema, &m, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	require.NotNil(t, lr)
	assert.NotEqual(t, collectionHash, lr.StorageRepresentation().GetHash())

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

	// create a value with invalid path
	jsonData, err = yaml.YAMLToJSON([]byte(invalidPathYaml))
	require.NoError(t, err)
	err = SaveValue(ctx, jsonData, nil, WithWorkspaceID(ws.WorkspaceID))
	if assert.Error(t, err) {
		t.Logf("Error: %v", err)
	}
}

func TestReferences(t *testing.T) {
	if !config.HierarchicalSchemas {
		t.Skip("Hierarchical schemas are not enabled")
	}
	emptyCollection1Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: valid
		catalog: example-catalog
		path: /
	`
	emptyCollection2Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: path
		catalog: example-catalog
		path: /valid
	`
	emptyCollection3Yaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: anotherpath
		catalog: example-catalog
		path: /valid
	`
	validParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	updatedParamYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 20
				  default: 2
	`
	validParamYaml2 := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema2
				  catalog: example-catalog
				  path: /valid
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
	`
	updatedParamAtNewPathYaml := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 3
				  default: 2
	`
	updatedParamAtNewPathYaml2 := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /valid/path
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 20
				  default: 2
	`
	updatedParamYamlAtGrandparent := `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: example-catalog
				  path: /
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 20
				  default: 2
	`
	validCollectionYaml := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validCollectionYaml2 := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/path
	spec:
		parameters:
			connectionAttempts:
				schema: integer-param-schema2
				default: 3
			connectionDelay:
				schema: integer-param-schema
				default: 7	
			maxRetries:
				schema: integer-param-schema
				default: 8
			maxDelay:
				dataType: Integer
				default: 1000
	`
	validCollectionYamlAtNewPath := `
	version: v1
	kind: CollectionSchema
	metadata:
		name: app-config-collection
		catalog: example-catalog
		path: /valid/anotherpath
	spec:
		parameters:
			connectionAttempts:
				schema: integer-param-schema2
				default: 3
			connectionDelay:
				schema: integer-param-schema
				default: 7	
			maxRetries:
				schema: integer-param-schema
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

	replaceTabsWithSpaces(&emptyCollection1Yaml)
	replaceTabsWithSpaces(&emptyCollection2Yaml)
	replaceTabsWithSpaces(&emptyCollection3Yaml)
	replaceTabsWithSpaces(&validParamYaml)
	replaceTabsWithSpaces(&updatedParamYaml)
	replaceTabsWithSpaces(&validParamYaml2)
	replaceTabsWithSpaces(&updatedParamAtNewPathYaml)
	replaceTabsWithSpaces(&updatedParamAtNewPathYaml2)
	replaceTabsWithSpaces(&updatedParamYamlAtGrandparent)
	replaceTabsWithSpaces(&validCollectionYaml)
	replaceTabsWithSpaces(&validCollectionYaml2)
	replaceTabsWithSpaces(&validCollectionYamlAtNewPath)

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

	// create the empty collections
	jsonData, err := yaml.YAMLToJSON([]byte(emptyCollection1Yaml))
	require.NoError(t, err)
	collectionSchema, err := NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection2Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)
	jsonData, err = yaml.YAMLToJSON([]byte(emptyCollection3Yaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	require.NoError(t, err)
	err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
	require.NoError(t, err)

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml))
	var paramFqn string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
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
	collectionSchema, err = NewSchema(ctx, jsonData, nil)

	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
	}

	// Create the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validParamYaml2))
	var paramFqn2 string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn2 = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
	}
	// update the collection schema to include another parameter
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml2))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 2)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn2}, {Name: paramFqn}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
	}
	// update the collection back
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYaml))
	require.NoError(t, err)
	collectionSchema, err = NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.Len(t, refs, 1)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.Len(t, refs, 0)
	}
	// update the parameter
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}})
	}
	// create a collection schema at new path
	jsonData, err = yaml.YAMLToJSON([]byte(validCollectionYamlAtNewPath))
	require.NoError(t, err)
	collectionSchema2, err := NewSchema(ctx, jsonData, nil)
	if assert.NoError(t, err) {
		err = SaveSchema(ctx, collectionSchema2, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema2.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn2}, {Name: paramFqn}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}, {Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
	}

	// update the parameter at a new path with lower max value
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamAtNewPathYaml))
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.Error(t, err)
		t.Logf("Error: %v", err)
	}

	// update the parameter at a new path with higher max value
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamAtNewPathYaml2))
	var paramFqn3 string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		paramFqn3 = r.FullyQualifiedName()
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		// get all references
		refs, err := getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn3)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema2.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}, {Name: paramFqn2}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn3}})
	}

	// update the parameter at the grandparent path
	jsonData, err = yaml.YAMLToJSON([]byte(updatedParamYamlAtGrandparent))
	var paramFqn4 string
	if assert.NoError(t, err) {
		r, err := NewSchema(ctx, jsonData, nil)
		require.NoError(t, err)
		err = SaveSchema(ctx, r, WithWorkspaceID(ws.WorkspaceID))
		require.NoError(t, err)
		paramFqn4 = r.FullyQualifiedName()
		// get all references
		refs, err := getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn4)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn3)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, paramFqn2)
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: collectionSchema2.FullyQualifiedName()}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema2.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn}, {Name: paramFqn2}})
		refs, err = getSchemaReferences(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, collectionSchema.FullyQualifiedName())
		require.NoError(t, err)
		assert.ElementsMatch(t, refs, []schemamanager.SchemaReference{{Name: paramFqn3}})
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
