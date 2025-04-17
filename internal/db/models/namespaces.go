package models

import (
	"github.com/google/uuid"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

/*
Table "public.namespaces"
   Column    |           Type           | Collation | Nullable | Default
-------------+--------------------------+-----------+----------+---------
 name        | character varying(128)   |           | not null |
 variant_id  | uuid                     |           | not null |
 catalog_id  | uuid                     |           | not null |
 tenant_id   | character varying(10)    |           | not null |
 description | character varying(1024)  |           |          |
 info        | jsonb                    |           |          |
 created_at  | timestamp with time zone |           |          | now()
 updated_at  | timestamp with time zone |           |          | now()
Indexes:
    "namespaces_pkey" PRIMARY KEY, btree (name, variant_id, catalog_id, tenant_id)
Check constraints:
    "namespaces_name_check" CHECK (name::text ~ '^[A-Za-z0-9_-]+$'::text)
Foreign-key constraints:
    "namespaces_tenant_id_fkey" FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
    "namespaces_variant_id_catalog_id_tenant_id_fkey" FOREIGN KEY (variant_id, catalog_id, tenant_id) REFERENCES variants(variant_id, catalog_id, tenant_id) ON DELETE CASCADE
Referenced by:
    TABLE "collections" CONSTRAINT "collections_namespace_variant_id_catalog_id_tenant_id_fkey" FOREIGN KEY (namespace, variant_id, catalog_id, tenant_id) REFERENCES namespaces(name, variant_id, catalog_id, tenant_id) ON DELETE CASCADE
    TABLE "workspace_collections" CONSTRAINT "workspace_collections_namespace_variant_id_catalog_id_tena_fkey" FOREIGN KEY (namespace, variant_id, catalog_id, tenant_id) REFERENCES namespaces(name, variant_id, catalog_id, tenant_id) ON DELETE CASCADE
*/

type Namespace struct {
	Name        string         `db:"name"`
	VariantID   uuid.UUID      `db:"variant_id"`
	CatalogID   uuid.UUID      `db:"catalog_id"`
	TenantID    types.TenantId `db:"tenant_id"`
	Description string         `db:"description"`
	Info        []byte         `db:"info"`
}
