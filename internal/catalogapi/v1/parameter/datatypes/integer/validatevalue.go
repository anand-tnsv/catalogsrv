package integer

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
)

func (is *Spec) ValidateValue(v any) apperrors.Error {
	val, ok := v.(int)
	if !ok {
		return apierrors.ErrInvalidType
	}
	if is.Validation == nil {
		return nil
	}
	iv := is.Validation
	if iv.MinValue != nil && val < *iv.MinValue {
		return apierrors.ErrValueBelowMin
	}

	if iv.MaxValue != nil && val > *iv.MaxValue {
		return apierrors.ErrValueAboveMax
	}

	if iv.Step != nil && *iv.Step != 0 {
		if *iv.Step > 0 && (val-*iv.MinValue)%*iv.Step != 0 {
			return apierrors.ErrValueNotInStep
		}

		if *iv.Step < 0 && (*iv.MaxValue-val)%*iv.Step != 0 {
			return apierrors.ErrValueNotInStep
		}
	}
	return nil
}
