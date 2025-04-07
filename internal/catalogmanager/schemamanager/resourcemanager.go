package schemamanager

import "github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"

type ResourceManager interface {
	Version() string
	Kind() string
	ParameterManager() ParameterManager
	CollectionManager() CollectionManager
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
