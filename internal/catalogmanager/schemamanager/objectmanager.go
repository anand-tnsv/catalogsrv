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
	ParameterManager() ParameterManager
	CollectionManager() CollectionManager
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
}

type ClosestParentObjectFinder func(ctx context.Context, t types.CatalogObjectType, targetName string) (path string, hash string, err apperrors.Error)
type ObjectLoaderByPath func(ctx context.Context, t types.CatalogObjectType, path string) (ObjectManager, apperrors.Error)
type ObjectLoaderByHash func(ctx context.Context, t types.CatalogObjectType, hash string, m ...ObjectMetadata) (ObjectManager, apperrors.Error)

type ObjectLoaders struct {
	ByPath        ObjectLoaderByPath
	ByHash        ObjectLoaderByHash
	ClosestParent ClosestParentObjectFinder
}
