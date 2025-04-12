package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type ParameterSpec struct {
	Name    string
	Default types.NullableAny
	Value   types.NullableAny
}

type CollectionManager interface {
	ParameterNames() []string
	ParametersWithSchema(schemaName string) []ParameterSpec
	ValidateDependencies(context.Context, ObjectLoaders, ObjectReferences) (ObjectReferences, apperrors.Error)
	ValidateValue(ctx context.Context, loaders ObjectLoaders, param string, value types.NullableAny) apperrors.Error
	SetValue(ctx context.Context, param string, value types.NullableAny) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
