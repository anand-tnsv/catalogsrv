package schemamanager

import (
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type Parameter interface {
	ValidateSpec() schemaerr.ValidationErrors
	ValidateValue(types.NullableAny) apperrors.Error
	DefaultValue() any
}
