package integer

import (
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/apierrors"
)

func (iv *Validation) Validate(val int) error {
	if iv.minValuePresent && val < iv.MinValue {
		return apierrors.ErrValueBelowMin
	}

	if iv.maxValuePresent && val > iv.MaxValue {
		return apierrors.ErrValueAboveMax
	}

	if iv.stepPresent && iv.Step != 0 {
		if iv.Step > 0 && (val-iv.MinValue)%iv.Step != 0 {
			return apierrors.ErrValueInvalid
		}

		if iv.Step < 0 && (iv.MaxValue-val)%iv.Step != 0 {
			return apierrors.ErrValueInvalid
		}
	}
	return nil
}
