package apis

import (
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
)

// Create a new resource object
func createObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest()
	}

	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	namespace := chi.URLParam(r, "namespaceName")
	workspace := chi.URLParam(r, "workspaceRef")

	n := catalogmanager.ResourceName{}
	catalogContext := common.CatalogContextFromContext(ctx)
	if workspace != "" {
		n.Workspace = workspace
	} else if catalogContext != nil {
		n.Workspace = catalogContext.WorkspaceLabel
		n.WorkspaceID = catalogContext.WorkspaceId
	}
	if variantName != "" {
		n.Variant = variantName
	} else if catalogContext != nil {
		n.Variant = catalogContext.Variant
		n.VariantID = catalogContext.VariantId
	}
	if catalogName != "" {
		n.Catalog = catalogName
	} else if catalogContext != nil {
		n.Catalog = catalogContext.Catalog
		n.CatalogID = catalogContext.CatalogId
	}
	if namespace != "" {
		n.Namespace = namespace
	} else if catalogContext != nil {
		n.Namespace = catalogContext.Namespace
	}

	rm, err := catalogmanager.ResourceManagerFromRequest(ctx, req, n)
	if err != nil {
		return nil, err
	}
	resourceLoc, err := rm.Create(ctx)
	if err != nil {
		return nil, err
	}
	rsp := &httpx.Response{
		StatusCode: http.StatusCreated,
		Location:   resourceLoc, //TODO: Implement location
		Response:   nil,
	}

	return rsp, nil
}
