package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestCreateVersion(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Test case: Successfully create a version
	version := models.Version{
		Label:       "v1",
		Description: "First version",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)

	// Verify that the version was created successfully
	retrievedVersion, err := DB(ctx).GetVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedVersion)
	assert.Equal(t, "v1", retrievedVersion.Label)

	// Test case: Create a version with invalid label (should fail due to check constraint)
	invalidLabelVersion := models.Version{
		Label:       "invalid label with spaces",
		Description: "This version should fail",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &invalidLabelVersion)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test case: Create a version with invalid catalog ID (should fail)
	invalidCatalogIDVersion := models.Version{
		Label:       "v2",
		Description: "This version should fail due to invalid catalog ID",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   uuid.New(),
	}
	err = DB(ctx).CreateVersion(ctx, &invalidCatalogIDVersion)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidCatalog)

	// Test case: Create a version with invalid variant ID (should fail)
	invalidVariantIDVersion := models.Version{
		Label:       "v3",
		Description: "This version should fail due to invalid variant ID",
		Info:        info,
		VariantID:   uuid.New(),
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &invalidVariantIDVersion)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidCatalog)

	// Test case: Create a duplicate version (should fail due to unique constraint when label is non-empty)
	duplicateVersion := models.Version{
		Label:       "v1", // same label as the first created version
		Description: "This version should fail due to duplicate label",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &duplicateVersion)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrAlreadyExists)

	// Test case: Create a version with empty label (should succeed even with same variant and catalog)
	emptyLabelVersion := models.Version{
		Label:       "",
		Description: "This version has an empty label",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &emptyLabelVersion)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, emptyLabelVersion.VersionNum, variant.VariantID, catalog.CatalogID)

	// Test case: Missing tenant ID in context (should fail)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).CreateVersion(ctxWithoutTenant, &version)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestGetVersion(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Create a version for testing
	version := models.Version{
		Label:       "v1",
		Description: "Test version",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)

	// Test case: Successfully retrieve the version
	retrievedVersion, err := DB(ctx).GetVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedVersion)
	assert.Equal(t, version.VersionNum, retrievedVersion.VersionNum)
	assert.Equal(t, "v1", retrievedVersion.Label)
	assert.Equal(t, "Test version", retrievedVersion.Description)
	assert.Equal(t, info, retrievedVersion.Info)
	assert.Equal(t, variant.VariantID, retrievedVersion.VariantID)
	assert.Equal(t, catalog.CatalogID, retrievedVersion.CatalogID)
	assert.Equal(t, tenantID, retrievedVersion.TenantID)

	// Test case: Retrieve a non-existent version (should return an error)
	nonExistentVersionNum := version.VersionNum + 999
	_, err = DB(ctx).GetVersion(ctx, nonExistentVersionNum, variant.VariantID, catalog.CatalogID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Retrieve with missing tenant ID in context (should return an error)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	_, err = DB(ctx).GetVersion(ctxWithoutTenant, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestDeleteVersion(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Create a version for testing
	version := models.Version{
		Label:       "v1",
		Description: "Test version for deletion",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)

	// Test case: Successfully delete the version
	err = DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.NoError(t, err)

	// Verify that the version was deleted by attempting to retrieve it
	_, err = DB(ctx).GetVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Attempt to delete a non-existent version (should return an error)
	nonExistentVersionNum := version.VersionNum + 999
	err = DB(ctx).DeleteVersion(ctx, nonExistentVersionNum, variant.VariantID, catalog.CatalogID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Delete with missing tenant ID in context (should return an error)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).DeleteVersion(ctxWithoutTenant, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestSetVersionLabel(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Create a version for testing
	version := models.Version{
		Label:       "v1",
		Description: "Test version for label update",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)

	// Test case: Successfully update the label
	newLabel := "v2"
	err = DB(ctx).SetVersionLabel(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID, newLabel)
	assert.NoError(t, err)

	// Verify that the label was updated successfully
	updatedVersion, err := DB(ctx).GetVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.NoError(t, err)
	assert.NotNil(t, updatedVersion)
	assert.Equal(t, newLabel, updatedVersion.Label)

	// Test case: Attempt to set a duplicate label (should fail due to unique constraint)
	duplicateVersion := models.Version{
		Label:       "v3",
		Description: "Another test version",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &duplicateVersion)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, duplicateVersion.VersionNum, variant.VariantID, catalog.CatalogID)

	err = DB(ctx).SetVersionLabel(ctx, duplicateVersion.VersionNum, variant.VariantID, catalog.CatalogID, newLabel) // setting label to already used "v2"
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrAlreadyExists)

	// Test case: Attempt to set an invalid label (should fail due to check constraint)
	invalidLabel := "invalid label with spaces"
	err = DB(ctx).SetVersionLabel(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID, invalidLabel)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test case: Attempt to set an empty label (should return an error)
	err = DB(ctx).SetVersionLabel(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID, "")
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)

	// Test case: Set label with missing tenant ID in context (should return an error)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).SetVersionLabel(ctxWithoutTenant, version.VersionNum, variant.VariantID, catalog.CatalogID, "new_label_with_missing_tenant")
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestUpdateVersionDescription(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Create a version for testing
	version := models.Version{
		Label:       "v1",
		Description: "Original description",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)

	// Test case: Successfully update the description
	newDescription := "Updated description"
	err = DB(ctx).UpdateVersionDescription(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID, newDescription)
	assert.NoError(t, err)

	// Verify that the description was updated successfully
	updatedVersion, err := DB(ctx).GetVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.NoError(t, err)
	assert.NotNil(t, updatedVersion)
	assert.Equal(t, newDescription, updatedVersion.Description)

	// Test case: Update description with a very long string (edge case)
	longDescription := strings.Repeat("A", 1024) // Assuming the max length is 1024 for this test
	err = DB(ctx).UpdateVersionDescription(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID, longDescription)
	assert.NoError(t, err)

	// Verify the long description was updated successfully
	updatedVersion, err = DB(ctx).GetVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	assert.NoError(t, err)
	assert.Equal(t, longDescription, updatedVersion.Description)

	// Test case: Attempt to update description on a non-existent version (should return an error)
	nonExistentVersionNum := version.VersionNum + 999
	err = DB(ctx).UpdateVersionDescription(ctx, nonExistentVersionNum, variant.VariantID, catalog.CatalogID, "Non-existent update")
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Update description with missing tenant ID in context (should return an error)
	ctxWithoutTenant := common.SetTenantIdInContext(ctx, "")
	err = DB(ctx).UpdateVersionDescription(ctxWithoutTenant, version.VersionNum, variant.VariantID, catalog.CatalogID, "Description with missing tenant")
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrInvalidInput)
}

func TestCountVersionsInCatalogAndVariant(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Add multiple versions for testing
	for i := 1; i <= 3; i++ {
		version := models.Version{
			Label:       fmt.Sprintf("v%d", i),
			Description: fmt.Sprintf("Test version %d", i),
			Info:        info,
			VariantID:   variant.VariantID,
			CatalogID:   catalog.CatalogID,
		}
		err = DB(ctx).CreateVersion(ctx, &version)
		assert.NoError(t, err)
		defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	}

	// Test case: Count versions in catalog and variant
	count, err := DB(ctx).CountVersionsInCatalogAndVariant(ctx, catalog.CatalogID, variant.VariantID)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// Test case: Count versions in a non-existent catalog and variant (should be zero)
	nonExistentCatalogID := uuid.New()
	nonExistentVariantID := uuid.New()
	count, err = DB(ctx).CountVersionsInCatalogAndVariant(ctx, nonExistentCatalogID, nonExistentVariantID)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestGetNamedVersions(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Add named versions (non-null label) for testing
	for i := 1; i <= 2; i++ {
		version := models.Version{
			Label:       fmt.Sprintf("v%d", i),
			Description: fmt.Sprintf("Test named version %d", i),
			Info:        info,
			VariantID:   variant.VariantID,
			CatalogID:   catalog.CatalogID,
		}
		err = DB(ctx).CreateVersion(ctx, &version)
		assert.NoError(t, err)
		defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)
	}

	// Add an unnamed version (null label) for testing
	unnamedVersion := models.Version{
		Label:       "", // This will be stored as NULL in the database
		Description: "Unnamed version",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &unnamedVersion)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, unnamedVersion.VersionNum, variant.VariantID, catalog.CatalogID)

	// Retrieve named versions
	namedVersions, err := DB(ctx).GetNamedVersions(ctx, catalog.CatalogID, variant.VariantID)
	assert.NoError(t, err)
	assert.NotNil(t, namedVersions)

	// Verify that only the named versions are returned (should be 2)
	assert.Equal(t, 2, len(namedVersions))
	for i, version := range namedVersions {
		expectedLabel := fmt.Sprintf("v%d", i+1)
		expectedDescription := fmt.Sprintf("Test named version %d", i+1)
		assert.Equal(t, expectedLabel, version.Label)
		assert.Equal(t, expectedDescription, version.Description)
	}
}
func TestGetVersionByLabel(t *testing.T) {
	// Initialize context with logger and database connection
	ctx := log.Logger.WithContext(context.Background())
	ctx = newDb(ctx)
	defer DB(ctx).Close(ctx)

	tenantID := types.TenantId("TABCDE")
	projectID := types.ProjectId("P12345")

	// Set the tenant ID and project ID in the context
	ctx = common.SetTenantIdInContext(ctx, tenantID)
	ctx = common.SetProjectIdInContext(ctx, projectID)

	// Create the tenant and project for testing
	err := DB(ctx).CreateTenant(ctx, tenantID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteTenant(ctx, tenantID)

	err = DB(ctx).CreateProject(ctx, projectID)
	assert.NoError(t, err)
	defer DB(ctx).DeleteProject(ctx, projectID)

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
	defer DB(ctx).DeleteCatalog(ctx, catalog.CatalogID, "")

	// Create a variant for testing
	variant := models.Variant{
		Name:        "test_variant",
		Description: "A test variant",
		CatalogID:   catalog.CatalogID,
		Info:        info,
	}
	err = DB(ctx).CreateVariant(ctx, &variant)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVariant(ctx, catalog.CatalogID, variant.VariantID, "")

	// Add a version with a unique label for testing
	version := models.Version{
		Label:       "v1",
		Description: "Version with label v1",
		Info:        info,
		VariantID:   variant.VariantID,
		CatalogID:   catalog.CatalogID,
	}
	err = DB(ctx).CreateVersion(ctx, &version)
	assert.NoError(t, err)
	defer DB(ctx).DeleteVersion(ctx, version.VersionNum, variant.VariantID, catalog.CatalogID)

	// Test case: Successfully retrieve the version by label
	retrievedVersion, err := DB(ctx).GetVersionByLabel(ctx, "v1", catalog.CatalogID, variant.VariantID)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedVersion)
	assert.Equal(t, version.VersionNum, retrievedVersion.VersionNum)
	assert.Equal(t, version.Label, retrievedVersion.Label)
	assert.Equal(t, version.Description, retrievedVersion.Description)
	assert.Equal(t, version.Info, retrievedVersion.Info)

	// Test case: Attempt to retrieve a version with a non-existent label
	_, err = DB(ctx).GetVersionByLabel(ctx, "non_existent_label", catalog.CatalogID, variant.VariantID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Attempt to retrieve a version with an invalid catalog ID (should return not found error)
	invalidCatalogID := uuid.New()
	_, err = DB(ctx).GetVersionByLabel(ctx, "v1", invalidCatalogID, variant.VariantID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)

	// Test case: Attempt to retrieve a version with an invalid variant ID (should return not found error)
	invalidVariantID := uuid.New()
	_, err = DB(ctx).GetVersionByLabel(ctx, "v1", catalog.CatalogID, invalidVariantID)
	assert.Error(t, err)
	assert.ErrorIs(t, err, dberror.ErrNotFound)
}
