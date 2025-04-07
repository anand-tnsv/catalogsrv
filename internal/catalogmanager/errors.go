package catalogmanager

import "github.com/mugiliam/common/apperrors"

var (
	ErrCatalogError         apperrors.Error = apperrors.New("error in processing catalog")
	ErrCatalogNotFound      apperrors.Error = ErrCatalogError.Msg("catalog not found")
	ErrResourceNotFound     apperrors.Error = ErrCatalogError.Msg("object not found")
	ErrUnableToLoadResource apperrors.Error = ErrCatalogError.Msg("unable to load object")
	ErrAlreadyExists        apperrors.Error = ErrCatalogError.Msg("object already exists")
)
