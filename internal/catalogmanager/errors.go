package catalogmanager

import "github.com/mugiliam/common/apperrors"

var (
	ErrCatalogError       apperrors.Error = apperrors.New("error in processing catalog")
	ErrCatalogNotFound    apperrors.Error = ErrCatalogError.Msg("catalog not found")
	ErrObjectNotFound     apperrors.Error = ErrCatalogError.Msg("object not found")
	ErrUnableToLoadObject apperrors.Error = ErrCatalogError.Msg("unable to load object")
	ErrAlreadyExists      apperrors.Error = ErrCatalogError.Msg("object already exists")
	ErrInvalidSchema      apperrors.Error = ErrCatalogError.Msg("invalid schema")
	ErrEmptyMetadata      apperrors.Error = ErrCatalogError.Msg("empty metadata")
	ErrInvalidProject     apperrors.Error = ErrCatalogError.Msg("invalid project")
	ErrInvalidCatalog     apperrors.Error = ErrCatalogError.Msg("invalid catalog")
	ErrInvalidVariant     apperrors.Error = ErrCatalogError.Msg("invalid variant")
	ErrInvalidWorkspace   apperrors.Error = ErrCatalogError.Msg("invalid workspace")
	ErrInvalidObject      apperrors.Error = ErrCatalogError.Msg("invalid object")
	ErrInvalidVersion     apperrors.Error = ErrCatalogError.Msg("invalid version")
	ErrVariantNotFound    apperrors.Error = ErrCatalogError.Msg("variant not found")
	ErrWorkspaceNotFound  apperrors.Error = ErrCatalogError.Msg("workspace not found")
)
