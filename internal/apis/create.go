package apis

import (
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
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
	workspace := chi.URLParam(r, "workspaceRef")
	n := catalogmanager.ResourceName{}
	if workspace != "" {
		n.Workspace = workspace
	}
	if variantName != "" {
		n.Variant = variantName
	}
	if catalogName != "" {
		n.Catalog = catalogName
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
