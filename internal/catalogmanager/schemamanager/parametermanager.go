package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type ParameterManager interface {
	DataType() string
	Default() any
	ValidateValue(types.NullableAny) apperrors.Error
	ValidateDependencies(ctx context.Context, loaders ObjectLoaders, collectionRefs ObjectReferences) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
