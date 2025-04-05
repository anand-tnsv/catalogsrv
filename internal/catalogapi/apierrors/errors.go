package apierrors

import "github.com/mugiliam/common/apperrors"

var (
	ErrSchemaValidation apperrors.Error = apperrors.New("error validating schema")
	ErrEmptySchema      apperrors.Error = ErrSchemaValidation.Msg("empty schema")
	ErrInvalidVersion   apperrors.Error = ErrSchemaValidation.Msg("invalid version")

	ErrValueValidation apperrors.Error = apperrors.New("error validating value")
	ErrInvalidType     apperrors.Error = ErrValueValidation.Msg("invalid type")
	ErrInvalidKind     apperrors.Error = ErrValueValidation.Msg("unsupported kind")
	ErrInvalidDataType apperrors.Error = ErrValueValidation.Msg("unsupported data type")
	ErrValueBelowMin   apperrors.Error = ErrValueValidation.Msg("value is below minimum")
	ErrValueAboveMax   apperrors.Error = ErrValueValidation.Msg("value is above maximum")
	ErrValueInvalid    apperrors.Error = ErrValueValidation.Msg("value failed validation")
	ErrValueNotInStep  apperrors.Error = ErrValueValidation.Msg("value not in step with min and max values")
)
