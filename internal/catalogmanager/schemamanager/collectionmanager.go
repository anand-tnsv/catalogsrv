package schemamanager

import "github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"

type CollectionManager interface {
	Schema() string
	Metadata() SchemaMetadata
	CollectionSchemaManager() CollectionSchemaManager
	CollectionSchema() []byte
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
