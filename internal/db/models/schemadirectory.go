package models

import "github.com/google/uuid"

/*
	   Column    |         Type          | Collation | Nullable |      Default
-----------------+-----------------------+-----------+----------+--------------------
	directory_id | uuid                  |           | not null | uuid_generate_v4()
	version_num  | integer               |           | not null |
	variant_id   | uuid                  |           | not null |
	catalog_id   | uuid                  |           | not null |
	tenant_id    | character varying(10) |           | not null |
	directory    | jsonb                 |           | not null |
*/

type SchemaDirectory struct {
	DirectoryID uuid.UUID `db:"directory_id"`
	VersionNum  int       `db:"version_num"`
	VariantID   uuid.UUID `db:"variant_id"`
	CatalogID   uuid.UUID `db:"catalog_id"`
	TenantID    string    `db:"tenant_id"`
	Directory   []byte    `db:"directory"` // JSONB
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
