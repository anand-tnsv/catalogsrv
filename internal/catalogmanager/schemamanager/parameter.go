package schemamanager

import (
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
)

type Parameter interface {
	ValidateSpec() schemaerr.ValidationErrors
	ValidateValue(any) apperrors.Error
	DefaultValue() any
}
