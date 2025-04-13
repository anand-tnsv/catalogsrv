package catalogmanager

import (
	"net/http"

	"github.com/mugiliam/common/apperrors"
)

var (
	ErrCatalogError              apperrors.Error = apperrors.New("error in processing catalog")
	ErrCatalogNotFound           apperrors.Error = ErrCatalogError.New("catalog not found").SetExpandError(true).SetStatusCode(http.StatusNotFound)
	ErrObjectNotFound            apperrors.Error = ErrCatalogError.New("object not found")
	ErrParentCollectionNotFound  apperrors.Error = ErrCatalogError.New("collection not found")
	ErrUnableToLoadObject        apperrors.Error = ErrCatalogError.New("unable to load object")
	ErrUnableToUpdateObject      apperrors.Error = ErrCatalogError.New("unable to update object").SetStatusCode(http.StatusInternalServerError)
	ErrAlreadyExists             apperrors.Error = ErrCatalogError.New("object already exists")
	ErrInvalidSchema             apperrors.Error = ErrCatalogError.New("invalid schema").SetExpandError(true).SetStatusCode(http.StatusBadRequest)
	ErrEmptyMetadata             apperrors.Error = ErrCatalogError.New("empty metadata")
	ErrInvalidProject            apperrors.Error = ErrCatalogError.New("invalid project")
	ErrInvalidCatalog            apperrors.Error = ErrCatalogError.New("invalid catalog")
	ErrInvalidVariant            apperrors.Error = ErrCatalogError.New("invalid variant")
	ErrInvalidWorkspace          apperrors.Error = ErrCatalogError.New("invalid workspace")
	ErrInvalidObject             apperrors.Error = ErrCatalogError.New("invalid object")
	ErrInvalidVersion            apperrors.Error = ErrCatalogError.New("invalid version")
	ErrVariantNotFound           apperrors.Error = ErrCatalogError.New("variant not found")
	ErrWorkspaceNotFound         apperrors.Error = ErrCatalogError.New("workspace not found")
	ErrInvalidVersionOrWorkspace apperrors.Error = ErrCatalogError.New("invalid version or workspace")
	ErrInvalidCollection         apperrors.Error = ErrCatalogError.New("invalid collection")
)
