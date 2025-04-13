package types

import "github.com/google/uuid"

type TenantId string
type ProjectId string
type CatalogId uuid.UUID

const DefaultVariant = "default"
const InitialVersionLabel = "init"

func (u CatalogId) String() string {
	return uuid.UUID(u).String()
}

func (u CatalogId) IsNil() bool {
	return u == CatalogId(uuid.Nil)
}

type CatalogObjectType string

const (
	CatalogObjectTypeInvalid                CatalogObjectType = "invalid"
	CatalogObjectTypeParameterSchema        CatalogObjectType = "parameter_schema"
	CatalogObjectTypeCollectionSchema       CatalogObjectType = "collection_schema"
	CatalogObjectTypeCatalogCollectionValue CatalogObjectType = "collection_value"
)

type Nullable interface {
	IsNil() bool
}
