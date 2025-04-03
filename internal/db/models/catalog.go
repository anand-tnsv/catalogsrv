package models

import (
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
)

// Catalog model definition
type Catalog struct {
	CatalogID   uuid.UUID    `db:"catalog_id"`
	Name        string       `db:"name"`
	Description string       `db:"description"`
	Info        pgtype.JSONB `db:"info"`
}
