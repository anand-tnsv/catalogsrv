package integer

import (
	"github.com/mugiliam/common/apperrors"
	v1errors "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

func (is *Spec) ValidateValue(v types.NullableAny) apperrors.Error {
	var val int
	if err := v.GetAs(&val); err != nil {
		return v1errors.ErrInvalidIntegerType
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
