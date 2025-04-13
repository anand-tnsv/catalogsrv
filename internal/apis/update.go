package apis

import (
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

// Create a new resource object
func updateObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")

	if variantName != "" {
		kind = types.VariantKind
	} else if catalogName != "" {
		kind = types.CatalogKind
	}

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest()
	}

	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	rm, err := catalogmanager.ResourceManagerFromName(ctx, kind, catalogmanager.ResourceName{
		Catalog: catalogName,
		Variant: variantName,
	})
	if err != nil {
		return nil, err
	}

	err = rm.Update(ctx, req)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   nil,
	}
	return rsp, nil
}
