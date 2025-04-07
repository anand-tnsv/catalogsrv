package schemamanager

import "github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"

type CollectionManager interface {
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
