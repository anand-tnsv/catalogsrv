package models

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

/*
 Column    |         Type          | Collation | Nullable |      Default
--------------+-----------------------+-----------+----------+--------------------
 directory_id | uuid                  |           | not null | uuid_generate_v4()
 version_num  | integer               |           |          |
 workspace_id | uuid                  |           |          |
 variant_id   | uuid                  |           | not null |
 catalog_id   | uuid                  |           | not null |
 tenant_id    | character varying(10) |           | not null |
 directory    | jsonb                 |           | not null |
Indexes:
    "collections_directory_pkey" PRIMARY KEY, btree (directory_id, tenant_id)
Foreign-key constraints:
    "collections_directory_tenant_id_fkey" FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
    "collections_directory_version_num_variant_id_catalog_id_te_fkey" FOREIGN KEY (version_num, variant_id, catalog_id, tenant_id) REFERENCES versions(version_num, variant_id, catalog_id, tenant_id)
    "collections_directory_workspace_id_tenant_id_fkey" FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE
*/

type SchemaDirectory struct {
	DirectoryID uuid.UUID      `db:"directory_id"`
	VersionNum  int            `db:"version_num"`
	WorkspaceID uuid.UUID      `db:"workspace_id"`
	VariantID   uuid.UUID      `db:"variant_id"`
	CatalogID   uuid.UUID      `db:"catalog_id"`
	TenantID    types.TenantId `db:"tenant_id"`
	Directory   []byte         `db:"directory"` // JSONB
}

type ObjectRef struct {
	Hash       string     `json:"hash"`
	References References `json:"references"`
}

// we'll keep Reference as a struct for future extensibility at the cost of increased storage space
type Reference struct {
	Name string `json:"name"`
}

type References []Reference
type Directory map[string]ObjectRef

func DirectoryToJSON(directory Directory) ([]byte, error) {
	return json.Marshal(directory)
}

func JSONToDirectory(data []byte) (Directory, error) {
	var directory Directory
	err := json.Unmarshal(data, &directory)
	return directory, err
}

/*
Directory is a json that has the following format:
{
	"<path>" : {
		"hash": "<hash>"
	}
	...
}
Here path is the path of the object in the form of /a/b/c/d
*/
