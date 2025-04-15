package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type ObjectManager interface {
	Version() string
	Kind() string
	Type() types.CatalogObjectType
	ParameterSchemaManager() ParameterSchemaManager
	CollectionSchemaManager() CollectionSchemaManager
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
	Metadata() ObjectMetadata
	Name() string
	Path() string
	FullyQualifiedName() string
	Catalog() string
	Description() string
	SetName(name string)
	SetPath(path string)
	SetCatalog(catalog string)
	SetDescription(description string)
	ToJson(ctx context.Context) ([]byte, apperrors.Error)
	Compare(other ObjectManager, excludeMetadata bool) bool
}

type ClosestParentObjectFinder func(ctx context.Context, t types.CatalogObjectType, targetName string) (path string, hash string, err apperrors.Error)
type ParameterReferenceForName func(name string) string
type ObjectLoaderByPath func(ctx context.Context, t types.CatalogObjectType, m *ObjectMetadata) (ObjectManager, apperrors.Error)
type ObjectLoaderByHash func(ctx context.Context, t types.CatalogObjectType, hash string, m *ObjectMetadata) (ObjectManager, apperrors.Error)
type SelfMetadata func() ObjectMetadata

type ObjectLoaders struct {
	ByPath        ObjectLoaderByPath
	ByHash        ObjectLoaderByHash
	ClosestParent ClosestParentObjectFinder
	ParameterRef  ParameterReferenceForName
	SelfMetadata  SelfMetadata
}
