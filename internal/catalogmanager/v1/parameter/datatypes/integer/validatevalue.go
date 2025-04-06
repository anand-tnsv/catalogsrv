package integer

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
)

func (is *Spec) ValidateValue(v any) apperrors.Error {
	val, ok := v.(int)
	if !ok {
		return validationerrors.ErrInvalidType
	}
	if is.Validation == nil {
		return nil
	}
	iv := is.Validation
	if iv.MinValue != nil && val < *iv.MinValue {
		return validationerrors.ErrValueBelowMin
	}

	if iv.MaxValue != nil && val > *iv.MaxValue {
		return validationerrors.ErrValueAboveMax
	}

	if iv.Step != nil && *iv.Step != 0 {
		if *iv.Step > 0 && (val-*iv.MinValue)%*iv.Step != 0 {
			return validationerrors.ErrValueNotInStep
		}

		if *iv.Step < 0 && (*iv.MaxValue-val)%*iv.Step != 0 {
			return validationerrors.ErrValueNotInStep
		}
	}
	return nil
}
