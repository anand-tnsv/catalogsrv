package db

import (
	"context"
	"testing"

	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestCreateTenant(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")

	// Test successful tenant creation
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	// Test trying to create the same tenant again (should return ErrAlreadyExists)
	err = DB(ctx).CreateTenant(ctx, tenantID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrAlreadyExists)
}

func TestGetTenant(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	// First, create the tenant to test retrieval
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)

	// Test successfully retrieving the created tenant
	tenant, err := DB(ctx).GetTenant(ctx, tenantID)
	assert.NoError(t, err)
	assert.NotNil(t, tenant)
	assert.Equal(t, tenantID, tenant.TenantID)

	// Test trying to get a non-existent tenant (should return ErrNotFound)
	nonExistentTenantID := types.TenantId("nonexistent123")
	tenant, err = DB(ctx).GetTenant(ctx, nonExistentTenantID)
	assert.Error(t, err)
	assert.Nil(t, tenant)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}

func TestCreateProject(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant to associate with the project
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	// Test creating a new project
	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

	// Test trying to create the same project (should return ErrAlreadyExists)
	err = DB(ctx).CreateProject(ctx, projectID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrAlreadyExists)

	// Test trying to create a project without a tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).CreateProject(ctx, projectID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrDatabase)
}

func TestGetProject(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant and project to test retrieval
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

	// Test successfully retrieving the created project
	project, err := DB(ctx).GetProject(ctx, projectID)
	assert.NoError(t, err)
	assert.NotNil(t, project)
	assert.Equal(t, projectID, project.ProjectID)
	assert.Equal(t, tenantID, project.TenantID)

	// Test trying to get a non-existent project (should return ErrNotFound)
	nonExistentProjectID := types.ProjectId("nonexistent123")
	project, err = DB(ctx).GetProject(ctx, nonExistentProjectID)
	assert.Error(t, err)
	assert.Nil(t, project)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}

func TestDeleteProject(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)

	// Create the tenant and project to test deletion
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

	// Test deleting the project
	err = DB(ctx).DeleteProject(ctx, projectID)
	assert.NoError(t, err)

	// Test trying to delete a non-existent project (should succeed without errors)
	err = DB(ctx).DeleteProject(ctx, projectID)
	assert.NoError(t, err)

	// Test trying to retrieve a deleted project (should return ErrNotFound)
	project, err := DB(ctx).GetProject(ctx, projectID)
	assert.Error(t, err)
	assert.Nil(t, project)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}

func newDb(c ...context.Context) context.Context {
	var ctx context.Context
	if len(c) > 0 {
		ctx = ConnCtx(c[0])
	} else {
		ctx = ConnCtx(context.Background())
	}
	return ctx
}
