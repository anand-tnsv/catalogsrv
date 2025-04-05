package apierrors

import "github.com/mugiliam/common/apperrors"

var (
	ErrSchemaValidation apperrors.Error = apperrors.New("error validating schema")
	ErrValueValidation  apperrors.Error = apperrors.New("error validating value")
	ErrValueBelowMin    apperrors.Error = ErrValueValidation.Msg("value is below minimum")
	ErrValueAboveMax    apperrors.Error = ErrValueValidation.Msg("value is above maximum")
	ErrValueInvalid     apperrors.Error = ErrValueValidation.Msg("value failed validation")
)
