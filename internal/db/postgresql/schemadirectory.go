package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

func (h *hatchCatalogDb) CreateSchemaDirectory(ctx context.Context, t types.CatalogObjectType, dir *models.SchemaDirectory) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	if dir.DirectoryID == uuid.Nil {
		dir.DirectoryID = uuid.New()
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

	dir.TenantID = tenantID

	tx, err := h.conn().BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to start transaction")
		return dberror.ErrDatabase.Err(err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	errDb := h.createSchemaDirectoryWithTransaction(ctx, t, dir, tx)
	if errDb != nil {
		tx.Rollback()
		return errDb
	}

	if err := tx.Commit(); err != nil {
		return dberror.ErrDatabase.Err(err)
	}
	return nil
}

func (h *hatchCatalogDb) SetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID, dir []byte) apperrors.Error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		UPDATE ` + tableName + `
		SET directory = $1
		WHERE directory_id = $2 AND tenant_id = $3;`

	_, err := h.conn().ExecContext(ctx, query, dir, id, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

func (h *hatchCatalogDb) GetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID) ([]byte, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	query := `
		SELECT directory
		FROM ` + tableName + `
		WHERE directory_id = $1 AND tenant_id = $2;`

	var dir []byte
	err := h.conn().QueryRowContext(ctx, query, id, tenantID).Scan(&dir)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("directory not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}
	return dir, nil
}

func (h *hatchCatalogDb) createSchemaDirectoryWithTransaction(ctx context.Context, t types.CatalogObjectType, dir *models.SchemaDirectory, tx *sql.Tx) apperrors.Error {
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}
	if dir.DirectoryID == uuid.Nil {
		dir.DirectoryID = uuid.New()
	}
	var refName string
	var refId any
	if dir.WorkspaceID != uuid.Nil {
		refName = "workspace_id"
		refId = dir.WorkspaceID
	} else if dir.VersionNum != 0 {
		refName = "version_num"
		refId = dir.VersionNum
	} else {
		return dberror.ErrInvalidInput.Msg("either workspace_id or version_num must be set")
	}

	// Insert the schema directory into the database and get created uuid
	query := ` INSERT INTO ` + tableName + ` (directory_id, ` + refName + `, variant_id, catalog_id, tenant_id, directory)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (directory_id, tenant_id) DO NOTHING RETURNING directory_id;`

	var directoryID uuid.UUID
	err := tx.QueryRowContext(ctx, query, dir.DirectoryID, refId, dir.VariantID, dir.CatalogID, dir.TenantID, dir.Directory).Scan(&directoryID)
	if err != nil {
		if err == sql.ErrNoRows {
			return dberror.ErrAlreadyExists.Msg("schema directory already exists")
		} else {
			return dberror.ErrDatabase.Err(err)
		}
	}

	dir.DirectoryID = directoryID

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

	query := `SELECT directory_id, variant_id, catalog_id, tenant_id, directory
		FROM ` + tableName + `
		WHERE directory_id = $1 AND tenant_id = $2;`

	dir := &models.SchemaDirectory{}
	err := h.conn().QueryRowContext(ctx, query, directoryID, tenantID).Scan(&dir.DirectoryID, &dir.VariantID, &dir.CatalogID, &dir.TenantID, &dir.Directory)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("schema directory not found")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}
	return dir, nil
}

func (h *hatchCatalogDb) GetObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (*models.ObjectRef, apperrors.Error) {
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
	err := h.conn().QueryRowContext(ctx, query, path, directoryID, tenantID).Scan(&objectData)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, dberror.ErrNotFound.Msg("object not found in directory")
		}
		return nil, dberror.ErrDatabase.Err(err)
	}

	if len(objectData) == 0 {
		return nil, dberror.ErrNotFound.Msg("object not found in directory")
	}

	var obj models.ObjectRef
	if err := json.Unmarshal(objectData, &obj); err != nil {
		return nil, dberror.ErrDatabase.Err(err)
	}

	return &obj, nil
}

func (h *hatchCatalogDb) AddOrUpdateObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, obj models.ObjectRef) apperrors.Error {
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

	result, err := h.conn().ExecContext(ctx, query, path, data, directoryID, tenantID)
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		// No matching row was found with directory_id and tenant_id
		return dberror.ErrNotFound.Msg("object not found")
	}

	// get object to verify update
	if o, err := h.GetObjectByPath(ctx, t, directoryID, path); err != nil {
		return err
	} else if o.Hash != obj.Hash {
		return dberror.ErrDatabase.Msg("object hash mismatch after update")
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

func (h *hatchCatalogDb) FindClosestObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, targetName, startPath string) (string, *models.ObjectRef, apperrors.Error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return "", nil, dberror.ErrMissingTenantID
	}
	tableName := getSchemaDirectoryTableName(t)
	if tableName == "" {
		return "", nil, dberror.ErrInvalidInput.Msg("invalid catalog object type")
	}

	// Build LIKE pattern for paths ending with "/<targetName>"
	likePattern := "%" + "/" + targetName

	// SQL to find paths ending in targetName, ordered by path length descending
	query := `
SELECT key AS path, value AS object
FROM ` + tableName + `, LATERAL jsonb_each_text(directory)
WHERE directory_id = $1 AND tenant_id = $2
  AND key LIKE $3
ORDER BY LENGTH(key) DESC;
`

	rows, err := h.conn().QueryContext(ctx, query, directoryID, tenantID, likePattern)
	if err != nil {
		return "", nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	var closestPath string
	var closestObject models.ObjectRef

	for rows.Next() {
		var path string
		var objectData []byte

		if err := rows.Scan(&path, &objectData); err != nil {
			return "", nil, dberror.ErrDatabase.Err(err)
		}
		fmt.Println(path)
		// Check if the path is equal to or a parent of startPath
		if isParentPath(path, startPath, targetName) {
			closestPath = path

			if err := json.Unmarshal(objectData, &closestObject); err != nil {
				return "", nil, dberror.ErrDatabase.Err(err)
			}
			break
		}
	}

	// Error handling for row scan
	if err := rows.Err(); err != nil {
		return "", nil, dberror.ErrDatabase.Err(err)
	}

	if closestPath == "" {
		return "", nil, nil
	}

	return closestPath, &closestObject, nil
}

// isParentPath checks if path is a parent of startPath.
func isParentPath(path, startPath, targetName string) bool {
	parentPath := strings.TrimSuffix(path, "/"+targetName)
	b := strings.HasPrefix(startPath, parentPath)
	if b {
		r := strings.TrimPrefix(startPath, parentPath)
		return r == "" || strings.HasPrefix(r, "/")
	}
	return false
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
