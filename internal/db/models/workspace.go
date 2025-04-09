package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

/*
           Column         |           Type           | Collation | Nullable |      Default
-----------------------+--------------------------+-----------+----------+--------------------
 workspace_id          | uuid                     |           | not null | uuid_generate_v4()
 label                 | character varying(128)   |           |          |
 description           | character varying(1024)  |           |          |
 info                  | jsonb                    |           |          |
 base_version          | integer                  |           | not null |
 parameters_directory  | uuid                     |           |          | uuid_nil()
 collections_directory | uuid                     |           |          | uuid_nil()
 variant_id            | uuid                     |           | not null |
 catalog_id            | uuid                     |           | not null |
 tenant_id             | character varying(10)    |           | not null |
 created_at            | timestamp with time zone |           |          | now()
 updated_at            | timestamp with time zone |           |          | now()
Indexes:
    "workspaces_pkey" PRIMARY KEY, btree (workspace_id, tenant_id)
    "workspaces_label_variant_id_catalog_id_tenant_id_key" UNIQUE CONSTRAINT, btree (label, variant_id, catalog_id, tenant_id)
Check constraints:
    "workspaces_label_check" CHECK (label IS NULL OR label::text ~ '^[A-Za-z0-9_]+$'::text)
Foreign-key constraints:
    "workspaces_base_version_variant_id_catalog_id_tenant_id_fkey" FOREIGN KEY (base_version, variant_id, catalog_id, tenant_id) REFERENCES versions(version_num, variant_id, catalog_id, tenant_id)
    "workspaces_variant_id_catalog_id_tenant_id_fkey" FOREIGN KEY (variant_id, catalog_id, tenant_id) REFERENCES variants(variant_id, catalog_id, tenant_id) ON DELETE CASCADE
Referenced by:
    TABLE "collections_directory" CONSTRAINT "collections_directory_workspace_id_tenant_id_fkey" FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE
    TABLE "parameters_directory" CONSTRAINT "parameters_directory_workspace_id_tenant_id_fkey" FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE
Triggers:
    update_workspaces_updated_at BEFORE UPDATE ON workspaces FOR EACH ROW EXECUTE FUNCTION set_updated_at()
*/

type Workspace struct {
	WorkspaceID    uuid.UUID      `db:"workspace_id"`
	Label          string         `db:"label"`
	Description    string         `db:"description"`
	Info           pgtype.JSONB   `db:"info"` // JSONB
	BaseVersion    int            `db:"base_version"`
	ParametersDir  uuid.UUID      `db:"parameters_directory"`
	CollectionsDir uuid.UUID      `db:"collections_directory"`
	VariantID      uuid.UUID      `db:"variant_id"`
	CatalogID      uuid.UUID      `db:"catalog_id"`
	TenantID       types.TenantId `db:"tenant_id"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      time.Time      `db:"updated_at"`
}
