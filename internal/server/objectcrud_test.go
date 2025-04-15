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
	"sigs.k8s.io/yaml"
)

func TestCatalogCreate(t *testing.T) {

	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

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

	// Check Location in header
	assert.Equal(t, "valid-catalog", response.Header().Get("Location"))

}

func TestGetUpdateDeleteCatalog(t *testing.T) {
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

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

	// Create a New Request to get the catalog
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil)

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
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/validcatalog", nil)
	response = executeTestRequest(t, httpReq, nil)
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
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog", nil)
	setRequestBodyAndHeader(t, httpReq, req)

	response = executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the catalog
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil)

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
	httpReq, _ = http.NewRequest("DELETE", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil)

	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the deleted catalog
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog", nil)
	response = executeTestRequest(t, httpReq, nil)

	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestVariantCrud(t *testing.T) {
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

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
	// Check Location in header
	assert.Equal(t, "valid-catalog/variants/valid-variant", response.Header().Get("Location"))

	// Get the variant
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant", nil)
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
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

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
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant", nil)
	setRequestBodyAndHeader(t, httpReq, req)

	response = executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the variant
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil)

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
	httpReq, _ = http.NewRequest("DELETE", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil)

	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a New Request to get the deleted variant
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil)

	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Get the variant
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
}

func TestWorkspaceCrud(t *testing.T) {
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

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

	// get this workspace
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
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
	err = json.Unmarshal([]byte(req), &reqType)
	assert.NoError(t, err)
	assert.Equal(t, reqType, rspType)

	// update the workspace
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"label": "valid-workspace",
			"catalog": "valid-catalog",
			"variant": "valid-variant",
			"base_version": 1,
			"description": "This is a new description"
		}
	}`
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated workspace
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	response = executeTestRequest(t, httpReq, nil)
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
	// Delete the workspace
	httpReq, _ = http.NewRequest("DELETE", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	response = executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// try to get the deleted workspace
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	response = executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusNotFound, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// Create a workspace without a label
	// Create a New Request
	httpReq, _ = http.NewRequest("POST", "/tenant/TABCDE/project/PABCDE/catalogs/create", nil)
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
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
			"catalog": "valid-catalog",
			"variant": "valid-variant",
			"base_version": 1,
			"description": "This is a valid workspace"
		}
	}`
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated workspace
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	response = executeTestRequest(t, httpReq, nil)
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

	// Set the label to empty
	req = `
	{
		"version": "v1",
		"kind": "Workspace",
		"metadata": {
			"label": "",
			"catalog": "valid-catalog",
			"variant": "valid-variant",
			"base_version": 1,
			"description": ""
		}
	}`
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	setRequestBodyAndHeader(t, httpReq, req)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated workspace
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	response = executeTestRequest(t, httpReq, nil)
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

	// Delete the workspace
	httpReq, _ = http.NewRequest("DELETE", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	response = executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// delete again
	httpReq, _ = http.NewRequest("DELETE", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id, nil)
	response = executeTestRequest(t, httpReq, nil)
	// Check the response code
	if !assert.Equal(t, http.StatusNoContent, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

}

func TestObjectCrud(t *testing.T) {
	ctx := newDb()
	defer db.DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("PABCDE")

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
			kind: Collection
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
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
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
		kind: Collection
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
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update just description
	reqYaml = `
		version: v1
		kind: Collection
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
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update the collection with a dataType
	reqYaml = `
		version: v1
		kind: Collection
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
	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
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
				kind: Parameter
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
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/parameter/integer-param-schema", nil)
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	// update the collection with an the newly created parameter
	reqYaml = `
		version: v1
		kind: Collection
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

	httpReq, _ = http.NewRequest("PUT", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
	setRequestBodyAndHeader(t, httpReq, string(reqJson))
	response = executeTestRequest(t, httpReq, nil)
	if !assert.Equal(t, http.StatusOK, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}
	// Get the updated object
	httpReq, _ = http.NewRequest("GET", "/tenant/TABCDE/project/PABCDE/catalogs/valid-catalog/variants/valid-variant/workspaces/"+id+"/collection/valid", nil)
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

}

func replaceTabsWithSpaces(s *string) {
	*s = strings.ReplaceAll(*s, "\t", "    ")
}
