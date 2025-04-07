package models

import (
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

/*
   Column    |          Type           | Collation | Nullable | Default
-------------+-------------------------+-----------+----------+---------
 version_num | integer                 |           | not null |
 label       | character varying(128)  |           |          |
 description | character varying(1024) |           |          |
 info        | jsonb                   |           |          |
 variant_id  | uuid                    |           | not null |
 catalog_id  | uuid                    |           | not null |
 project_id  | character varying(10)   |           | not null |
 tenant_id   | character varying(10)   |           | not null |
*/

type Version struct {
	VersionNum  int             `db:"version_num"`
	Label       string          `db:"label"`
	Description string          `db:"description"`
	Info        pgtype.JSONB    `db:"info"` // JSONB
	VariantID   uuid.UUID       `db:"variant_id"`
	CatalogID   uuid.UUID       `db:"catalog_id"`
	ProjectID   types.ProjectId `db:"project_id"`
	TenantID    types.TenantId  `db:"tenant_id"`
}
