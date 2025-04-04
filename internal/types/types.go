package types

import "github.com/google/uuid"

type TenantId string
type ProjectId string
type CatalogId uuid.UUID

func (u CatalogId) String() string {
	return uuid.UUID(u).String()
}

func (u CatalogId) IsNil() bool {
	return u == CatalogId(uuid.Nil)
}
