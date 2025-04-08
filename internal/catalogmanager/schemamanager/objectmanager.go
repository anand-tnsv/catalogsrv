package schemamanager

import "github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"

type ObjectManager interface {
	Version() string
	Kind() string
	ParameterManager() ParameterManager
	CollectionManager() CollectionManager
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
