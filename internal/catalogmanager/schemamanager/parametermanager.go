package schemamanager

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type ParameterManager interface {
	DataType() string
	Default() any
	ValidateValue(types.NullableAny) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
