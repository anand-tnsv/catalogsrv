package types

import "github.com/google/uuid"

type TenantId string
type ProjectId string
type CatalogId uuid.UUID

const DefaultVariant = "default"
const InitialVersionLabel = "init"
const DefaultNamespace = "default"

func (u CatalogId) String() string {
	return uuid.UUID(u).String()
}

func (u CatalogId) IsNil() bool {
	return u == CatalogId(uuid.Nil)
}

const (
	CatalogKind          = "Catalog"
	VariantKind          = "Variant"
	WorkspaceKind        = "Workspace"
	ParameterSchemaKind  = "ParameterSchema"
	CollectionSchemaKind = "CollectionSchema"
	CollectionKind       = "Collection"
	ValueKind            = "Value"
)

const (
	ObjectTypeParameter  = "parameterschema"
	ObjectTypeCollection = "collectionschema"
	ObjectTypeValue      = "value"
)

var validObjTypes = []string{ObjectTypeCollection, ObjectTypeParameter, ObjectTypeValue}

func InValidObjectTypes(s string) bool {
	for _, v := range validObjTypes {
		if s == v {
			return true
		}
	}
	return false
}

const (
	VersionV1 = "v1"
)

type CatalogObjectType string

const (
	CatalogObjectTypeInvalid           CatalogObjectType = "invalid"
	CatalogObjectTypeUnknown           CatalogObjectType = "unknown"
	CatalogObjectTypeParameterSchema   CatalogObjectType = "parameter_schema"
	CatalogObjectTypeCollectionSchema  CatalogObjectType = "collection_schema"
	CatalogObjectTypeCatalogCollection CatalogObjectType = "collection"
)

type Nullable interface {
	IsNil() bool
}
