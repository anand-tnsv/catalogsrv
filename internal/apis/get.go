package apis

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

func getObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	workspaceName := chi.URLParam(r, "workspaceName")

	if workspaceName != "" {
		kind = types.WorkspaceKind
	} else if variantName != "" {
		kind = types.VariantKind
	} else if catalogName != "" {
		kind = types.CatalogKind
	}

	rm, err := catalogmanager.ResourceManagerFromName(ctx, kind, catalogmanager.ResourceName{
		Catalog:   catalogName,
		Variant:   variantName,
		Workspace: workspaceName, // either label or workspace ID is accepted and will be parsed later
	})
	if err != nil {
		return nil, err
	}

	rsrc, err := rm.Get(ctx)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   rsrc,
	}
	return rsp, nil
}
