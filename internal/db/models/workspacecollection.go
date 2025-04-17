package models

import (
	"github.com/google/uuid"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

/*
	Table "public.workspace_collections"
	Column       |           Type           | Collation | Nullable |           Default
	-------------------+--------------------------+-----------+----------+------------------------------
	collection_id     | uuid                     |           | not null | uuid_generate_v4()
	path              | character varying(512)   |           | not null |
	hash              | character(128)           |           | not null |
	description       | character varying(1024)  |           |          |
	namespace         | character varying(128)   |           | not null | 'default'::character varying
	collection_schema | character varying(512)   |           | not null |
	info              | jsonb                    |           |          |
	workspace_id      | uuid                     |           | not null |
	variant_id        | uuid                     |           | not null |
	catalog_id        | uuid                     |           | not null |
	tenant_id         | character varying(10)    |           | not null |
	created_at        | timestamp with time zone |           |          | now()
	Indexes:
	"workspace_collections_pkey" PRIMARY KEY, btree (collection_id, tenant_id)
	"idx_schema_namespace_workspace_variant_catalog_tenant" btree (collection_schema, namespace, workspace_id, variant_id, catalog_id, tenant_id)
	"workspace_collections_path_namespace_variant_id_catalog_id__key" UNIQUE CONSTRAINT, btree (path, namespace, variant_id, catalog_id, tenant_id)
	Check constraints:
	"workspace_collections_collection_schema_check" CHECK (collection_schema::text ~ '^[A-Za-z0-9_-]+$'::text)
	"workspace_collections_namespace_check" CHECK (namespace::text ~ '^[A-Za-z0-9_-]+$'::text)
	"workspace_collections_path_check" CHECK (path::text ~ '^(/[A-Za-z0-9_-]+)+$'::text)
	Foreign-key constraints:
	"workspace_collections_namespace_variant_id_catalog_id_tena_fkey" FOREIGN KEY (namespace, variant_id, catalog_id, tenant_id) REFERENCES namespaces(name, variant_id, catalog_id, tenant_id) ON DELETE CASCADE
	"workspace_collections_tenant_id_fkey" FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
	"workspace_collections_workspace_id_tenant_id_fkey" FOREIGN KEY (workspace_id, tenant_id) REFERENCES workspaces(workspace_id, tenant_id) ON DELETE CASCADE
*/

type WorkspaceCollection struct {
	CollectionID     uuid.UUID      `db:"collection_id"`
	Path             string         `db:"path"`
	Hash             string         `db:"hash"`
	Description      string         `db:"description"`
	Namespace        string         `db:"namespace"`
	CollectionSchema string         `db:"collection_schema"`
	Info             []byte         `db:"info"`
	WorkspaceID      uuid.UUID      `db:"workspace_id"`
	VariantID        uuid.UUID      `db:"variant_id"`
	CatalogID        uuid.UUID      `db:"catalog_id"`
	TenantID         types.TenantId `db:"tenant_id"`
}
