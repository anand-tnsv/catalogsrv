package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"sigs.k8s.io/yaml"
)

func TestCatalogCreate(t *testing.T) {

	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	testContext := TestContext{
		TenantId:  tenantID,
		ProjectId: projectID,
	}

	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)

	req := `
{
	"version": "v1",
	"kind": "Catalog",
	"metadata": {
		"name": "valid-catalog",
		"description": "This is a valid catalog"
	}
} `
	setRequestBodyAndHeader(t, httpReq, req)

	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Check Location in header
	assert.Contains(t, response.Header().Get("Location"), "/catalogs/valid-catalog")
}

func TestGetUpdateDeleteCatalog(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	testContext := TestContext{
		TenantId:  tenantID,
		ProjectId: projectID,
	}

	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)

	req := `
{
	"version": "v1",
	"kind": "Catalog",
	"metadata": {
		"name": "valid-catalog",
		"description": "This is a valid catalog"
	}
} `
	setRequestBodyAndHeader(t, httpReq, req)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	checkHeader(t, response.Header())

	rspType := make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)

	reqType := make(map[string]any)
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// Create a New Request to get a non-existing catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/validcatalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	t.Logf("Response: %v", response.Body.String())
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.FailNow()
	}

	// Update the catalog
	req = `
{
	"version": "v1",
	"kind": "Catalog",
	"metadata": {
		"name": "valid-catalog",
		"description": "This is a new description"
	}
} `
	httpReq, _ = http.NewRequest("PUT", "/catalogs/valid-catalog", nil)
	setRequestBodyAndHeader(t, httpReq, req)

	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())

	rspType = make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)

	reqType = make(map[string]any)
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// Delete the catalog
	httpReq, _ = http.NewRequest("DELETE", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the deleted catalog
	httpReq, _ = http.NewRequest("GET", "/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestVariantCrud(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	testContext := TestContext{
		TenantId:       tenantID,
		ProjectId:      projectID,
		CatalogContext: common.CatalogContext{},
	}

	// Create a catalog
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a variant
	httpReq, _ = http.NewRequest("POST", "/catalogs", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant",
				"catalog": "valid-catalog",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Check Location in header
	assert.Contains(t, response.Header().Get("Location"), "/variants")
	loc := response.Header().Get("Location")

	// Get the variant
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())

	rspType := make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)

	reqType := make(map[string]any)
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// Create a new variant on the /variants endpoint
	httpReq, _ = http.NewRequest("POST", "/variants", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant2",
				"catalog": "valid-catalog",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Check Location in header
	assert.Contains(t, response.Header().Get("Location"), "/variants/")
	loc = response.Header().Get("Location")
	// Get the variant
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspType = make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)

	reqType = make(map[string]any)
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// Create a new variant by updating the testcontext
	testContext.CatalogContext.Catalog = "invalid-catalog"
	req, _ = sjson.Set(req, "metadata.variant", "valid-variant-3")
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusBadRequest, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Update the variant
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant",
				"catalog": "valid-catalog",
				"description": "This is a new description"
			}
		}`
	testContext.CatalogContext.Catalog = "valid-catalog"
	httpReq, _ = http.NewRequest("PUT", "/variants/valid-variant", nil)
	setRequestBodyAndHeader(t, httpReq, req)

	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the variant
	httpReq, _ = http.NewRequest("GET", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())

	rspType = make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)

	reqType = make(map[string]any)
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// Delete the variant
	httpReq, _ = http.NewRequest("DELETE", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the deleted variant
	httpReq, _ = http.NewRequest("GET", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)

	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Get the variant
	httpReq, _ = http.NewRequest("GET", "/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestNamespaceCrud(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	testContext := TestContext{
		TenantId:       tenantID,
		ProjectId:      projectID,
		CatalogContext: common.CatalogContext{},
	}

	// Create a catalog
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a variant
	httpReq, _ = http.NewRequest("POST", "/catalogs", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant",
				"catalog": "valid-catalog",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a namespace
	httpReq, _ = http.NewRequest("POST", "/namespaces?c=valid-catalog&v=valid-variant", nil)
	req = `
		{
			"version": "v1",
			"kind": "Namespace",
			"metadata": {
				"name": "valid-namespace",
				"description": "This is a valid namespace"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Check Location in header
	assert.Equal(t, "/namespaces/valid-namespace", response.Header().Get("Location"))
	// Get the namespace
	httpReq, _ = http.NewRequest("GET", "/namespaces/valid-namespace?v=valid-variant&c=valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestWorkspaceCrud(t *testing.T) {
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	testContext := TestContext{
		TenantId:       tenantID,
		ProjectId:      projectID,
		CatalogContext: common.CatalogContext{},
	}

	// Create a catalog
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/catalogs", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a variant
	httpReq, _ = http.NewRequest("POST", "/variants?catalog=valid-catalog", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Create a workspace
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/workspaces?c=valid-catalog&v=valid-variant", nil)
	req = `
		{
			"version": "v1",
			"kind": "Workspace",
			"metadata": {
				"label": "valid-workspace",
				"description": "This is a valid workspace"
			}
	}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response. should contain the workspace id. Test for if the id is a valid uuid
	loc := response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	id := loc[strings.LastIndex(loc, "/")+1:]
	_, err = uuid.Parse(id)
	assert.Nil(t, err)

	// get this workspace
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	// update the workspace
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"label": "valid-workspace",
			"description": "This is a new description"
		}
	}`
	testContext.CatalogContext.Catalog = "valid-catalog"
	testContext.CatalogContext.Variant = "valid-variant"
	httpReq, _ = http.NewRequest("PUT", "/workspaces/valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated workspace
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())

	// Delete the workspace
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted workspace
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a workspace without a label
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/workspaces", nil)
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"description": "This is a valid workspace"
		}
	}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// check the location header in response. should contain the workspace id. Test for if the id is a valid uuid
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	id = loc[strings.LastIndex(loc, "/")+1:]
	_, err = uuid.Parse(id)
	assert.Nil(t, err)

	// set a valid label for the workspace
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"label": "valid-workspace",
			"description": "This is a valid workspace"
		}
	}`
	httpReq, _ = http.NewRequest("PUT", loc, nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated workspace
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	label := gjson.Get(response.Body.String(), "metadata.label").String()
	assert.Equal(t, "valid-workspace", label)

	// Set the label to empty
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"label": "",
			"description": ""
		}
	}`
	httpReq, _ = http.NewRequest("PUT", loc, nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated workspace
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	label = gjson.Get(response.Body.String(), "metadata.label").String()
	assert.Equal(t, "", label)

	// Delete the workspace
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// delete again
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

}

func TestObjectCrud(t *testing.T) {
	t.Skip()
	ctx := newDb()
	t.Cleanup(func() {
		db.DB(ctx).Close(ctx)
	})
	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant for testing
	err := db.DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		_ = db.DB(ctx).DeleteTenant(ctx, tenantID)
	})

	// Create the project for testing
	err = db.DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer db.DB(ctx).DeleteProject(ctx, projectID)

	// Create a catalog
	// Create a New Request
	httpReq, _ := http.NewRequest("POST", "/tenant/TABCDE/project/PABCDE/catalogs/create", nil)
	req := `
		{
			"version": "v1",
			"kind": "Catalog",
			"metadata": {
				"name": "valid-catalog",
				"description": "This is a valid catalog"
			}
		} `
	setRequestBodyAndHeader(t, httpReq, req)
	// Execute Request
	response := executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a variant
	httpReq, _ = http.NewRequest("POST", "/tenant/TABCDE/project/PABCDE/catalogs/create", nil)
	req = `
		{
			"version": "v1",
			"kind": "Variant",
			"metadata": {
				"name": "valid-variant",
				"catalog": "valid-catalog",
				"description": "This is a valid variant"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a workspace
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/tenant/TABCDE/project/PABCDE/catalogs/create", nil)
	req = `
		{
			"version": "v1",
			"kind": "Workspace",
			"metadata": {
				"label": "valid-workspace",
				"catalog": "valid-catalog",
				"variant": "valid-variant",
				"base_version": 1,
				"description": "This is a valid workspace"
			}
		}`
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response. should contain the workspace id. Test for if the id is a valid uuid
	loc := response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	id := loc[strings.LastIndex(loc, "/")+1:]
	_, err = uuid.Parse(id)
	assert.Nil(t, err)

	// Create an object
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/create", nil)
	reqYaml := `
			version: v1
			kind: CollectionSchema
			metadata:
				name: valid
				catalog: valid-catalog
				variant: valid-variant
				path: /
				description: This is a valid collection
			spec: {}
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err := yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response. should contain the workspace id. Test for if the id is a valid uuid
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)

	// Get the object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspType := make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)
	reqType := make(map[string]any)
	err = json.Unmarshal(reqJson, &reqType)
	assert.NoError(t, err)
	if !assert.Equal(t, reqType, rspType) {
		b, _ := yaml.JSONToYAML(response.Body.Bytes())
		t.Logf("Response: %s", string(b))
		t.FailNow()
	}

	// Update the object
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a valid collection
		spec: {}
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update just description
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a new description
		spec: {}
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update the collection with a dataType
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a new description
		spec:
			parameters:
				maxDelay:
					schema: ""
					dataType: Integer
					default: 1000
					annotations:
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	rspType = make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)
	reqType = make(map[string]any)
	err = json.Unmarshal(reqJson, &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// create a valid parameter
	reqYaml = `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: valid-catalog
				  path: /
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 10
				  default: 5
			`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("POST", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/create", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)

	// Get the parameter
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/parameterschema/integer-param-schema", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update the collection with an the newly created parameter
	reqYaml = `
		version: v1
		kind: CollectionSchema
		metadata:
			name: valid
			catalog: valid-catalog
			variant: valid-variant
			path: /
			description: This is a new description
		spec:
			parameters:
				maxDelay:
					schema: ""
					dataType: Integer
					default: 1000
					annotations:
				maxRetries:
					schema: integer-param-schema
					dataType: ""
					default: 8
					annotations:
	`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)

	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collectionschema/valid", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	rspType = make(map[string]any)
	err = json.Unmarshal(response.Body.Bytes(), &rspType)
	assert.NoError(t, err)
	reqType = make(map[string]any)
	err = json.Unmarshal(reqJson, &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// modify the parameter to a smaller max value
	reqYaml = `
				version: v1
				kind: ParameterSchema
				metadata:
				  name: integer-param-schema
				  catalog: valid-catalog
				  path: /
				spec:
				  dataType: Integer
				  validation:
				    minValue: 1
				    maxValue: 5
				  default: 5
			`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/parameterschema/integer-param-schema", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.NotEqual(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

}

func replaceTabsWithSpaces(s *string) {
	*s = strings.ReplaceAll(*s, "\t", "    ")
}
