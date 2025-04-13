package apis

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

func deleteObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")

	if variantName != "" {
		kind = types.VariantKind
	} else if catalogName != "" {
		kind = types.CatalogKind
	}

	rm, err := catalogmanager.ResourceManagerFromName(ctx, kind, catalogmanager.ResourceName{
		Catalog: catalogName,
		Variant: variantName,
	})
	if err != nil {
		return nil, err
	}

	err = rm.Delete(ctx)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusNoContent,
		Response:   nil,
	}
	return rsp, nil
}
