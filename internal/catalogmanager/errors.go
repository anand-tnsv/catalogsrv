package catalogmanager

import (
	"net/http"

	"github.com/mugiliam/common/apperrors"
)

var (
	ErrCatalogError              apperrors.Error = apperrors.New("error in processing catalog").SetStatusCode(http.StatusInternalServerError)
	ErrCatalogNotFound           apperrors.Error = ErrCatalogError.New("catalog not found").SetExpandError(true).SetStatusCode(http.StatusNotFound)
	ErrObjectNotFound            apperrors.Error = ErrCatalogError.New("object not found").SetStatusCode(http.StatusNotFound)
	ErrParentCollectionNotFound  apperrors.Error = ErrCatalogError.New("collection not found").SetStatusCode(http.StatusNotFound)
	ErrUnableToLoadObject        apperrors.Error = ErrCatalogError.New("unable to load object").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToUpdateObject      apperrors.Error = ErrCatalogError.New("unable to update object").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToDeleteObject      apperrors.Error = ErrCatalogError.New("unable to delete object").SetStatusCode(http.StatusInternalServerError)
	ErrAlreadyExists             apperrors.Error = ErrCatalogError.New("object already exists").SetStatusCode(http.StatusConflict)
	ErrInvalidSchema             apperrors.Error = ErrCatalogError.New("invalid schema").SetExpandError(true).SetStatusCode(http.StatusBadRequest)
	ErrEmptyMetadata             apperrors.Error = ErrCatalogError.New("empty metadata").SetStatusCode(http.StatusBadRequest)
	ErrInvalidProject            apperrors.Error = ErrCatalogError.New("invalid project").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCatalog            apperrors.Error = ErrCatalogError.New("invalid catalog").SetStatusCode(http.StatusBadRequest)
	ErrInvalidVariant            apperrors.Error = ErrCatalogError.New("invalid variant").SetStatusCode(http.StatusBadRequest)
	ErrInvalidWorkspace          apperrors.Error = ErrCatalogError.New("invalid workspace").SetStatusCode(http.StatusBadRequest)
	ErrInvalidObject             apperrors.Error = ErrCatalogError.New("invalid object").SetStatusCode(http.StatusBadRequest)
	ErrInvalidVersion            apperrors.Error = ErrCatalogError.New("invalid version").SetStatusCode(http.StatusBadRequest)
	ErrVariantNotFound           apperrors.Error = ErrCatalogError.New("variant not found").SetStatusCode(http.StatusNotFound)
	ErrWorkspaceNotFound         apperrors.Error = ErrCatalogError.New("workspace not found").SetStatusCode(http.StatusNotFound)
	ErrInvalidVersionOrWorkspace apperrors.Error = ErrCatalogError.New("invalid version or workspace").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCollection         apperrors.Error = ErrCatalogError.New("invalid collection").SetStatusCode(http.StatusBadRequest)
	ErrInvalidUUID               apperrors.Error = ErrCatalogError.New("invalid uuid")
)
