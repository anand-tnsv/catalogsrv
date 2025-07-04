package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
)

type ResourceManager interface {
	Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error)
	Get(ctx context.Context) ([]byte, apperrors.Error)
	Delete(ctx context.Context) apperrors.Error
	Update(ctx context.Context, rsrcJson []byte) apperrors.Error
	Location() string
}
