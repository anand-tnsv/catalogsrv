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
	httpReq, _ = http.NewRequest("POST", "/variants", nil)
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
	httpReq, _ = http.NewRequest("GET", loc+"?v=valid-variant&c=valid-catalog", nil)
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
	httpReq, _ = http.NewRequest("POST", "/variants", nil)
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
	testContext.CatalogContext.Catalog = "valid-catalog"
	httpReq, _ = http.NewRequest("POST", "/variants", nil)
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
	testContext.CatalogContext.Variant = "valid-variant"

	// create a namespace
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
	// Create a workspace
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/workspaces", nil)
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

	// Create an object
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/collectionschemas", nil)
	reqYaml := `
			version: v1
			kind: CollectionSchema
			metadata:
				name: valid
				path: /
				description: This is a valid collection
			spec: {}
		`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err := yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)

	// Get the object
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson := response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a valid collection")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec").String(), "{}")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")

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
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
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
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a new description")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec").String(), "{}")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")

	// update a non-existing collection
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/invalid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	reqJson, _ = sjson.SetBytes(reqJson, "spec.parameters.garbage", "true")
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusBadRequest, response.Code) {
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
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	// compare the spec
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.schema").String(), "")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.dataType").String(), "Integer")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.default").String(), "1000")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.parameters.maxDelay.annotations").String(), "")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a new description")

	// send the same update request to the namespace
	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid?n=valid-namespace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// create the schema in the namespace
	httpReq, _ = http.NewRequest("POST", "/collectionschemas?n=valid-namespace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/collectionschemas/valid?namespace=valid-namespace", loc)

	// Get the object
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "valid-namespace")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.description").String(), "This is a new description")

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
	paramReqJson := reqJson
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("POST", "/parameterschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/parameterschemas/integer-param-schema?namespace=valid-namespace&workspace=valid-workspace", loc)

	// Get the parameter
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// add this parameter to root
	httpReq, _ = http.NewRequest("POST", "/parameterschemas", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
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

	httpReq, _ = http.NewRequest("PUT", "/collectionschemas/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// add this collection to workspace and namespace
	httpReq, _ = http.NewRequest("POST", "/collectionschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

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
	httpReq, _ = http.NewRequest("PUT", "/parameterschemas/integer-param-schema", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.NotEqual(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Test collections
	reqYaml = `
		version: v1
		kind: Collection
		metadata:
			name: my-collection
			path: /some/random/path
		spec:
			schema: valid
			values:
				maxRetries: 3
				maxAttempts: 10
	`
	replaceTabsWithSpaces(&reqYaml)
	reqJson, err = yaml.YAMLToJSON([]byte(reqYaml))
	require.NoError(t, err)
	httpReq, _ = http.NewRequest("POST", "/collections", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/collections/some/random/path/my-collection", loc)

	// create it again
	httpReq, _ = http.NewRequest("POST", "/collections", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusConflict, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// get the collection
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "my-collection")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/some/random/path")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.schema").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxRetries").String(), "3")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxAttempts").String(), "10")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.catalog").String(), "valid-catalog")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.variant").String(), "valid-variant")

	// update the collection
	reqJson, _ = sjson.SetBytes(reqJson, "spec.values.maxRetries", 5)
	httpReq, _ = http.NewRequest("PUT", loc, nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// get the collection
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	checkHeader(t, response.Header())
	rspJson = response.Body.Bytes()
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.name").String(), "my-collection")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.path").String(), "/some/random/path")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.schema").String(), "valid")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxRetries").String(), "5")
	assert.Equal(t, gjson.GetBytes(rspJson, "spec.values.maxAttempts").String(), "10")
	assert.Equal(t, gjson.GetBytes(rspJson, "metadata.namespace").String(), "")

	// update the collection with a non-existing schema
	collReqJson := reqJson
	reqJson, _ = sjson.SetBytes(reqJson, "spec.schema", "invalid")
	httpReq, _ = http.NewRequest("PUT", loc, nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusBadRequest, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update a non existing collection
	httpReq, _ = http.NewRequest("PUT", "/collections/invalid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// delete the collection
	httpReq, _ = http.NewRequest("DELETE", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted collection
	httpReq, _ = http.NewRequest("GET", loc, nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// delete the collection schema
	httpReq, _ = http.NewRequest("DELETE", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted collection
	httpReq, _ = http.NewRequest("GET", "/collectionschemas/valid", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// create the collection in a namespace and workspace
	httpReq, _ = http.NewRequest("POST", "/collections?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(collReqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusCreated, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// check the location header in response
	loc = response.Header().Get("Location")
	assert.NotEmpty(t, loc)
	assert.Equal(t, "/collections/some/random/path/my-collection?namespace=valid-namespace&workspace=valid-workspace", loc)

	// delete the parameter
	httpReq, _ = http.NewRequest("DELETE", "/parameterschemas/integer-param-schema", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted parameter
	httpReq, _ = http.NewRequest("GET", "/parameterschemas/integer-param-schema", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// create the parameter in the namespace again
	httpReq, _ = http.NewRequest("POST", "/parameterschemas?n=valid-namespace&workspace=valid-workspace", nil)
	setRequestBodyAndHeader(t, httpReq, string(paramReqJson))
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusConflict, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// delete the collection
	httpReq, _ = http.NewRequest("DELETE", "/collections/some/random/path/my-collection?namespace=valid-namespace&workspace=valid-workspace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// delete the collection schema
	httpReq, _ = http.NewRequest("DELETE", "/collectionschemas/valid?namespace=valid-namespace&workspace=valid-workspace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// delete the parameter in the namespace
	httpReq, _ = http.NewRequest("DELETE", "/parameterschemas/integer-param-schema?workspace=valid-workspace&namespace=valid-namespace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted parameter
	httpReq, _ = http.NewRequest("GET", "/parameterschemas/integer-param-schema?workspace=valid-workspace&namespace=valid-namespace", nil)
	response = executeTestRequest(t, httpReq, nil, testContext)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

}

func replaceTabsWithSpaces(s *string) {
	*s = strings.ReplaceAll(*s, "\t", "    ")
}
