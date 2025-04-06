package schemamanager

import "github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"

type CollectionManager interface {
	Name() string
	Catalog() string
	Path() string
	StorageRepresentation() schemastore.SchemaStorageRepresentation
}
