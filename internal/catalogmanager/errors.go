package catalogmanager

import "github.com/mugiliam/common/apperrors"

var (
	ErrCatalogError       apperrors.Error = apperrors.New("error in processing catalog")
	ErrCatalogNotFound    apperrors.Error = ErrCatalogError.Msg("catalog not found")
	ErrObjectNotFound     apperrors.Error = ErrCatalogError.Msg("object not found")
	ErrUnableToLoadObject apperrors.Error = ErrCatalogError.Msg("unable to load object")
	ErrAlreadyExists      apperrors.Error = ErrCatalogError.Msg("object already exists")
	ErrInvalidSchema      apperrors.Error = ErrCatalogError.Msg("invalid schema")
	ErrInvalidProject     apperrors.Error = ErrCatalogError.Msg("invalid project")
)
