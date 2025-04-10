package schemamanager

import "github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"

type ObjectManager interface {
	Version() string
	Kind() string
	ParameterManager() ParameterManager
	CollectionManager() CollectionManager
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
	Metadata() ObjectMetadata
	Name() string
	Path() string
	Catalog() string
	Description() string
	SetName(name string)
	SetPath(path string)
	SetCatalog(catalog string)
	SetDescription(description string)
}
