package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type CollectionManager interface {
	Schema() string
	Metadata() SchemaMetadata
	CollectionSchemaManager() CollectionSchemaManager
	CollectionSchema() []byte
	SetCollectionSchemaManager(csm CollectionSchemaManager)
	SetDefaultValues() apperrors.Error
	SetValue(ctx context.Context, schemaLoaders SchemaLoaders, param string, value types.NullableAny) apperrors.Error
	ValidateValues(ctx context.Context, schemaLoaders SchemaLoaders, currentValues ParamValues) apperrors.Error
	Values() ParamValues
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
