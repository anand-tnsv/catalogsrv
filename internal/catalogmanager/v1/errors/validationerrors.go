package errors

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
)

var (
	ErrInvalidIntegerType apperrors.Error = validationerrors.ErrInvalidType.Msg("invalid type for Integer")
)
