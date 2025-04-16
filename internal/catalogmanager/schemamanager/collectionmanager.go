package schemamanager

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
)

type CollectionManager interface {
	Schema() string
	Metadata() SchemaMetadata
	CollectionSchemaManager() CollectionSchemaManager
	CollectionSchema() []byte
	SetCollectionSchemaManager(csm CollectionSchemaManager)
	SetDefaultValues() apperrors.Error
	Values() ParamValues
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
