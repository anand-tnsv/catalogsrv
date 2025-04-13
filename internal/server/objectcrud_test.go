package server

import (
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
		"name": "ValidCatalog",
		"description": "This is a valid catalog"
	}
} `
	setRequestBodyAndHeader(t, httpReq, req)

	// Execute Request
	response := executeTestRequest(t, httpReq, nil)

	// Check the response code
	if !assert.Equal(t, http.StatusAccepted, response.Code) {
		t.Logf("Response: %v", response.Body.String())
		t.FailNow()
	}

	/*
		// Check headers
		checkHeader(t, response.Result().Header)

		compareJson(t,
			&api.GetVersionRsp{
				ServerVersion: "CatalogSrv: 1.0.0", //TODO - Implement server versioning
				ApiVersion:    api.ApiVersion_1_0,
			}, response.Body.String())
	*/
}
