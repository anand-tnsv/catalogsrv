package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestObjectCreate(t *testing.T) {

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
