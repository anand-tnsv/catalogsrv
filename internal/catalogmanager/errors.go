package catalogmanager

import (
	"net/http"

	"github.com/mugiliam/common/apperrors"
)

var (
	ErrCatalogError                          apperrors.Error = apperrors.New("error in processing catalog").SetStatusCode(http.StatusInternalServerError)
	ErrCatalogNotFound                       apperrors.Error = ErrCatalogError.New("catalog not found").SetExpandError(true).SetStatusCode(http.StatusNotFound)
	ErrObjectNotFound                        apperrors.Error = ErrCatalogError.New("object not found").SetStatusCode(http.StatusNotFound)
	ErrParentCollectionSchemaNotFound        apperrors.Error = ErrCatalogError.New("collection schema not found").SetStatusCode(http.StatusNotFound)
	ErrUnableToLoadObject                    apperrors.Error = ErrCatalogError.New("unable to load object").SetStatusCode(http.StatusInternalServerError)
	ErrUnableToUpdateObject                  apperrors.Error = ErrCatalogError.New("unable to update object").SetExpandError(true).SetStatusCode(http.StatusInternalServerError)
	ErrUnableToDeleteObject                  apperrors.Error = ErrCatalogError.New("unable to delete object").SetStatusCode(http.StatusInternalServerError)
	ErrAlreadyExists                         apperrors.Error = ErrCatalogError.New("object already exists").SetStatusCode(http.StatusConflict)
	ErrEqualToExistingObject                 apperrors.Error = ErrCatalogError.New("no change to existing object").SetStatusCode(http.StatusConflict)
	ErrInvalidSchema                         apperrors.Error = ErrCatalogError.New("invalid schema").SetExpandError(true).SetStatusCode(http.StatusBadRequest)
	ErrEmptyMetadata                         apperrors.Error = ErrCatalogError.New("empty metadata").SetStatusCode(http.StatusBadRequest)
	ErrInvalidProject                        apperrors.Error = ErrCatalogError.New("invalid project").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCatalog                        apperrors.Error = ErrCatalogError.New("invalid catalog").SetStatusCode(http.StatusBadRequest)
	ErrInvalidVariant                        apperrors.Error = ErrCatalogError.New("invalid variant").SetStatusCode(http.StatusBadRequest)
	ErrInvalidWorkspace                      apperrors.Error = ErrCatalogError.New("invalid workspace").SetStatusCode(http.StatusBadRequest)
	ErrInvalidObject                         apperrors.Error = ErrCatalogError.New("invalid object").SetStatusCode(http.StatusBadRequest)
	ErrInvalidVersion                        apperrors.Error = ErrCatalogError.New("invalid version").SetStatusCode(http.StatusBadRequest)
	ErrVariantNotFound                       apperrors.Error = ErrCatalogError.New("variant not found").SetStatusCode(http.StatusNotFound)
	ErrWorkspaceNotFound                     apperrors.Error = ErrCatalogError.New("workspace not found").SetStatusCode(http.StatusNotFound)
	ErrInvalidVersionOrWorkspace             apperrors.Error = ErrCatalogError.New("invalid version or workspace").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCollectionSchema               apperrors.Error = ErrCatalogError.New("invalid collection schema").SetStatusCode(http.StatusBadRequest)
	ErrInvalidCollection                     apperrors.Error = ErrCatalogError.New("invalid collection").SetStatusCode(http.StatusBadRequest)
	ErrSchemaOfCollectionNotMutable          apperrors.Error = ErrCatalogError.New("schema of a collection cannot be modified").SetStatusCode(http.StatusBadRequest)
	ErrInvalidUUID                           apperrors.Error = ErrCatalogError.New("invalid uuid")
	ErrNoAncestorReferencesFound             apperrors.Error = ErrUnableToDeleteObject.New("no ancestor references found").SetStatusCode(http.StatusConflict)
	ErrUnableToDeleteParameterWithReferences apperrors.Error = ErrUnableToDeleteObject.New("parameter has existing references in collections").SetStatusCode(http.StatusConflict)
	ErrInvalidParameter                      apperrors.Error = ErrCatalogError.New("invalid parameter").SetStatusCode(http.StatusBadRequest)
)
