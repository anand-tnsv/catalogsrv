package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type CollectionManager interface {
	ParameterSchemaReferences() []string
	ValidateDependencies(context.Context, ObjectLoaders) apperrors.Error
	ValidateValue(ctx context.Context, loaders ObjectLoaders, param string, value types.NullableAny) apperrors.Error
	SetValue(ctx context.Context, param string, value types.NullableAny) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
