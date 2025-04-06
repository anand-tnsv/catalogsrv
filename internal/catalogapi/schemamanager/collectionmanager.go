package schemamanager

import "github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schemamanager/schemastore"

type CollectionManager interface {
	Name() string
	Catalog() string
	Path() string
	StorageRepresentation() schemastore.SchemaStorageRepresentation
}
