package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

func (h *hatchCatalogDb) CreateSchemaDirectory(ctx context.Context, t types.CatalogObjectType, dir *models.SchemaDirectory) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if dir.DirectoryID == uuid.Nil {
		dir.DirectoryID = uuid.New()
	}
	if dir.VersionNum <= 0 {
		return dberror.ErrInvalidInput.Msg("version_num must be greater than 0")
	}
	if dir.VariantID == uuid.Nil {
		return dberror.ErrInvalidInput.Msg("variant_id cannot be empty")
	}
	if dir.CatalogID == uuid.Nil {
		return dberror.ErrInvalidInput.Msg("catalog_id cannot be empty")
	}
	if dir.TenantID == "" {
		return dberror.ErrInvalidInput.Msg("tenant_id cannot be empty")
	}
	if len(dir.Directory) == 0 {
		return dberror.ErrInvalidInput.Msg("directory cannot be nil")
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Insert the schema directory into the database and get created uuid
	query := ` INSERT INTO ` + tableName + ` (directory_id, version_num, variant_id, catalog_id, tenant_id, directory)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (directory_id, tenant_id) DO NOTHING RETURNING directory_id;`

	var directoryID uuid.UUID
	err := h.conn().QueryRowContext(ctx, query, dir.DirectoryID, dir.VersionNum, dir.VariantID, dir.CatalogID, tenantID, dir.Directory).Scan(&directoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			return dberror.ErrAlreadyExists.Msg("schema directory already exists")
		} else {
			return dberror.ErrDatabase.Err(err)
		}
	}
	return nil
}

func (h *hatchCatalogDb) GetSchemaDirectory(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID) (*models.SchemaDirectory, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `SELECT directory_id, version_num, variant_id, catalog_id, tenant_id, directory
		FROM ` + tableName + `
		WHERE directory_id = $1 AND tenant_id = $2;`

	dir := &models.SchemaDirectory{}
	err := h.conn().QueryRowContext(ctx, query, directoryID, tenantID).Scan(&dir.DirectoryID, &dir.VersionNum, &dir.VariantID, &dir.CatalogID, &dir.TenantID, &dir.Directory)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("schema directory not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}
	return dir, nil
}

func (h *hatchCatalogDb) GetObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (map[string]any, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		SELECT directory-> $1 AS object
		FROM ` + tableName + `
		WHERE directory_id = $2 AND tenant_id = $3;`

	var objectData []byte
	err := h.conn().QueryRowContext(ctx, query, path, directoryID).Scan(&objectData)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("object not found in directory")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	var obj map[string]any
	if err := json.Unmarshal(objectData, &obj); err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}

	return obj, nil
}

func (h *hatchCatalogDb) UpdateObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, obj map[string]any) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Convert the object to JSON
	data, err := json.Marshal(obj)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	query := `
		UPDATE ` + tableName + `
		SET directory = jsonb_set(directory, ARRAY[$1], $2::jsonb)
		WHERE directory_id = $3 AND tenant_id = $4;`

	_, err = h.conn().ExecContext(ctx, query, path, data, directoryID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

func (h *hatchCatalogDb) DeleteObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (bool, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return false, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return false, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Update and return whether the key was removed
	query := `
		UPDATE ` + tableName + `
		SET directory = directory - $1
		WHERE directory_id = $2 AND tenant_id = $3 AND directory ? $1
		RETURNING TRUE;
	`

	var wasRemoved bool
	err := h.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&wasRemoved)
	if err == sql.ErrNoRows {
		// Key did not exist, so nothing was removed
		return false, nil
	} else if err != nil {
		return false, dberror.ErrDatabase.Err(err)
	}

	return wasRemoved, nil
}

// FindClosestObject searches for an object in a JSONB directory that is associated with the specified targetName
// and located at the closest matching path to the provided startPath. It traverses outward from the startPath
// to the nearest parent paths until it finds a match.
//
// Parameters:
// - ctx: The context for handling deadlines, cancellation signals, and other request-scoped values.
// - t: The type of catalog object, used to identify the correct table within the schema.
// - directoryID: The unique identifier of the directory in which to search for the targetName.
// - targetName: The specific key name to search for within paths in the JSONB directory.
// - startPath: The initial path from which the search begins, traversing outward to locate the closest match.
//
// Returns:
// - string: The path in the directory that is closest to startPath and contains targetName as the last path segment.
// - map[string]any: The object associated with the closest matching path.
// - apperrors.Error: Error, if any occurs during execution.
//
// How It Works:
// 1. Constructs a LIKE pattern using targetName to match paths that end with "/<targetName>" in the JSONB directory.
// 2. Queries the directory for all paths ending in the specified targetName and orders them by path length in descending order.
// 3. For each matching path, it checks if the path is either equal to or a parent path of the startPath.
// 4. Returns the first match (closest path) and its associated object, if found.
//
// Example:
// Assume the directory JSON contains paths like "/a/b/c/d" and "/a/d":
//
//     path, object, err := h.FindClosestObject(ctx, catalogType, directoryID, "d", "/a/b/c")
//     This returns path="/a/b/c/d" and the object associated with "/a/b/c/d"
//
// If no path with the specified targetName is found within or above startPath, the function returns an empty string and nil for the object.

func (h *hatchCatalogDb) FindClosestObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, targetName, startPath string) (string, map[string]any, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return "", nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Construct the LIKE pattern to match paths that end with "/<targetName>"
	likePattern := "%" + "/" + targetName

	// Query to retrieve paths that match the LIKE pattern and order them by length
	query := `
		SELECT jsonb_object_keys(directory) AS path, directory->jsonb_object_keys(directory) AS object
		FROM ` + tableName + `
		WHERE directory_id = $1 AND tenant_id = $2 AND jsonb_object_keys(directory) LIKE $3
		ORDER BY LENGTH(jsonb_object_keys(directory)) DESC;
	`

	rows, err := h.conn().QueryContext(ctx, query, directoryID, tenantID, likePattern)
	if err != nil {
		return "", nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	// Track the closest match
	var closestPath string
	var closestObject map[string]any

	// Iterate over rows and find the first path that matches the startPath
	for rows.Next() {
		var path string
		var objectData []byte

		if err := rows.Scan(&path, &objectData); err != nil {
			return "", nil, dberror.ErrDatabase.Err(err)
		}

		// Check if the path is equal to or a parent of startPath
		if strings.HasPrefix(startPath, strings.TrimSuffix(path, "/"+targetName)) {
			closestPath = path

			// Deserialize the object JSON data into a map
			if err := json.Unmarshal(objectData, &closestObject); err != nil {
				return "", nil, dberror.ErrDatabase.Err(err)
			}

			// Since rows are ordered by descending path length, the first match is the closest
			break
		}
	}

	// Check for row errors
	if err := rows.Err(); err != nil {
		return "", nil, dberror.ErrDatabase.Err(err)
	}

	if closestPath == "" {
		// No matching path found
		return "", nil, nil
	}

	return closestPath, closestObject, nil
}

func (h *hatchCatalogDb) PathExists(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (bool, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return false, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return false, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		SELECT directory ? $1 AS exists
		FROM ` + tableName + `
		WHERE directory_id = $2 AND tenant_id = $3;`

	var exists bool
	err := h.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&exists)
	if err != nil {
		return false, dberror.ErrDatabase.Err(err)
	}

	return exists, nil
}

func getSchemaDirectoryTableName(t types.CatalogObjectType) string {
	switch t {
	case types.CatalogObjectTypeCollectionSchema:
		return "collections_directory"
	case types.CatalogObjectTypeParameterSchema:
		return "parameters_directory"
	default:
		return ""
	}
}
