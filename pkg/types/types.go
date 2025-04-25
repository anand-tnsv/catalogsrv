package types

import "github.com/google/uuid"

type TenantId string
type ProjectId string
type CatalogId uuid.UUID
type Hash string

const DefaultVariant = "default"
const InitialVersionLabel = "init"
const DefaultNamespace = "--root--"

func (u CatalogId) String() string {
	return uuid.UUID(u).String()
}

func (u CatalogId) IsNil() bool {
	return u == CatalogId(uuid.Nil)
}

const (
	CatalogKind          = "Catalog"
	VariantKind          = "Variant"
	NamespaceKind        = "Namespace"
	WorkspaceKind        = "Workspace"
	ParameterSchemaKind  = "ParameterSchema"
	CollectionSchemaKind = "CollectionSchema"
	CollectionKind       = "Collection"
	ValueKind            = "Value"
	InvalidKind          = "InvalidKind"
)

const (
	ResourceNameCatalogs          = "catalogs"
	ResourceNameVariants          = "variants"
	ResourceNameNamespaces        = "namespaces"
	ResourceNameWorkspaces        = "workspaces"
	ResourceNameParameterSchemas  = "parameterschemas"
	ResourceNameCollectionSchemas = "collectionschemas"
	ResourceNameCollections       = "collections"
	ResourceNameValues            = "values"
)

func Kind(t CatalogObjectType) string {
	switch t {
	case CatalogObjectTypeParameterSchema:
		return ParameterSchemaKind
	case CatalogObjectTypeCollectionSchema:
		return CollectionSchemaKind
	case CatalogObjectTypeCatalogCollection:
		return CollectionKind
	default:
		return ""
	}
}

func KindFromResourceName(uri string) string {
	switch uri {
	case ResourceNameCatalogs:
		return CatalogKind
	case ResourceNameVariants:
		return VariantKind
	case ResourceNameNamespaces:
		return NamespaceKind
	case ResourceNameWorkspaces:
		return WorkspaceKind
	case ResourceNameParameterSchemas:
		return ParameterSchemaKind
	case ResourceNameCollectionSchemas:
		return CollectionSchemaKind
	case ResourceNameCollections:
		return CollectionKind
	default:
		return InvalidKind
	}
}

func ResourceNameFromObjectType(t CatalogObjectType) string {
	switch t {
	case CatalogObjectTypeParameterSchema:
		return "parameterschemas"
	case CatalogObjectTypeCollectionSchema:
		return "collectionschemas"
	case CatalogObjectTypeCatalogCollection:
		return "collections"
	default:
		return ""
	}
}

var validObjTypes = []string{ResourceNameCollections, ResourceNameParameterSchemas, ResourceNameCollectionSchemas, ResourceNameValues}

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
	CatalogObjectTypeSchema            CatalogObjectType = "schema"
	CatalogObjectTypeParameterSchema   CatalogObjectType = "parameter_schema"
	CatalogObjectTypeCollectionSchema  CatalogObjectType = "collection_schema"
	CatalogObjectTypeCatalogCollection CatalogObjectType = "collection"
)

func CatalogObjectTypeFromKind(k string) CatalogObjectType {
	switch k {
	case ParameterSchemaKind:
		return CatalogObjectTypeParameterSchema
	case CollectionSchemaKind:
		return CatalogObjectTypeCollectionSchema
	case CollectionKind:
		return CatalogObjectTypeCatalogCollection
	default:
		return CatalogObjectTypeInvalid
	}
}

type Nullable interface {
	IsNil() bool
}

var TestContextKey = struct{}{}
