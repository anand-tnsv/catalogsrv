package db

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestCommitWorkspace(t *testing.T) {
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	t.Cleanup(func() {
		DB(ctx).Close(ctx)
	})

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	t.Cleanup(func() {
		DB(ctx).DeleteTenant(ctx, tenantID)
	})

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)

	var info pgtype.JSONB
	err = info.Set(`{"key": "value"}`)
	assert.NoError(t, err)

	// Create the catalog for testing
	catalog := models.Catalog{
		Name:        "test_catalog",
		Description: "A test catalog",
		Info:        info,
	}
	err = DB(ctx).CreateCatalog(ctx, &catalog)
	assert.NoError(t, err)

	variant, err := DB(ctx).GetVariant(ctx, catalog.CatalogID, uuid.Nil, types.DefaultVariant)
	assert.NoError(t, err)

	// create a workspace
	workspace := models.Workspace{
		Description: "A test workspace",
		VariantID:   variant.VariantID,
		Info:        pgtype.JSONB{Status: pgtype.Null},
		BaseVersion: 1,
	}
	err = DB(ctx).CreateWorkspace(ctx, &workspace)
	assert.NoError(t, err)

	// get the parameter directory
	pd := workspace.ParametersDir

	dirJson := `
	{
		"/z/a/b/c": {"hash": "a4f7d5b6e8c3d2a9f6e4b3c7d9a5f8b2e6c9d3f5a7e4b8c6d3a9f5e7d2f8b4a6"},
		"/a/b/c2/e/f": {"hash": "7b774effe4a349c6dd82ad4f4f21d34c22b8c323a4df9e20d3d4c61daceca69c"},
		"/a1/b/c/d": {"hash": "e3d4b5a7c6f5e4b3a8f7d2c3b4f6a5d8c7e3b6d9a4c8f3d5a2b6f7e4d3c5a8e9"},
		"/a1/b2/c": {"hash": "d3e6f5b4a9c8f2d7b3e5a6f4c8b7a9e3f5d2b6c4a8f7e9d3a5c6b8e7d2f4c5a9"},
		"/x/y1": {"hash": "8ad13a24fce736b8364d6574b4f9d4a8d2e4f8e0a8d4e5c7c7d6b4a8c3d2a5b6"}
	}
	`
	dir, err := models.JSONToDirectory([]byte(dirJson))
	assert.NoError(t, err)
	err = DB(ctx).SetDirectory(ctx, types.CatalogObjectTypeParameterSchema, pd, []byte(dirJson))
	assert.NoError(t, err)
	// get the directory
	dirRetJson, err := DB(ctx).GetDirectory(ctx, types.CatalogObjectTypeParameterSchema, pd)
	assert.NoError(t, err)
	dirRet, err := models.JSONToDirectory(dirRetJson)
	assert.NoError(t, err)
	assert.Equal(t, dir, dirRet)
	assert.Equal(t, dirRet["/x/y1"], dir["/x/y1"])

	// commit some collections
	// add some new objects
	cd := workspace.CollectionsDir
	// add a new object
	paths := []string{"/col/a/b", "/col/a/b/c", "/col/a/b/c/d/e/f", "/col/a/b/c/d", "/col/a/b/d/e", "/col/a/c", "/col/a/c/e/f"}
	refsP := models.References{
		{Name: "/par/a/b"}, {Name: "/par/a/b/c"}, {Name: "/par/a/b/c/d/c"}, {Name: "/par/a/b/c/d"}, {Name: "/par/a/b/d/e"}, {Name: "/par/a/c"}, {Name: "/par/a/c/e/f"},
	}
	for _, path := range paths {
		err = DB(ctx).AddOrUpdateObjectByPath(ctx, types.CatalogObjectTypeCollectionSchema, cd, path, models.ObjectRef{Hash: "hash", References: refsP})
		assert.NoError(t, err)
	}

	paths = []string{"/par/a/b", "/par/a/b/c", "/par/a/b/c/d/c", "/par/a/b/c/d", "/par/a/b/d/e", "/par/a/c", "/par/a/c/e/f"}
	refsC := models.References{
		{Name: "/col/a/b"}, {Name: "/col/a/b/c"}, {Name: "/col/a/b/c/d/e/f"}, {Name: "/col/a/b/c/d"}, {Name: "/col/a/b/d/e"}, {Name: "/col/a/c"}, {Name: "/col/a/c/e/f"},
	}
	for _, path := range paths {
		err = DB(ctx).AddOrUpdateObjectByPath(ctx, types.CatalogObjectTypeParameterSchema, pd, path, models.ObjectRef{Hash: "hash", References: refsC})
		assert.NoError(t, err)
	}

	err = DB(ctx).CommitWorkspace(ctx, &workspace)
	assert.NoError(t, err)
	// fetching this workspace should return an error
	_, err = DB(ctx).GetWorkspace(ctx, workspace.WorkspaceID)
	assert.Error(t, err)
	// create another workspace
	err = DB(ctx).CreateWorkspace(ctx, &workspace)
	assert.NoError(t, err)
	// get the directory
	dirRetJson, err = DB(ctx).GetDirectory(ctx, types.CatalogObjectTypeParameterSchema, workspace.ParametersDir)
	assert.NoError(t, err)
	_, err = models.JSONToDirectory(dirRetJson)
	assert.NoError(t, err)

	// now set an empty directory
	err = DB(ctx).SetDirectory(ctx, types.CatalogObjectTypeParameterSchema, workspace.ParametersDir, []byte("{}"))
	assert.NoError(t, err)

	// commit the workspace
	err = DB(ctx).CommitWorkspace(ctx, &workspace)
	assert.NoError(t, err)
	// get the directory
	_, err = DB(ctx).GetDirectory(ctx, types.CatalogObjectTypeParameterSchema, pd)
	assert.Error(t, err)
}
