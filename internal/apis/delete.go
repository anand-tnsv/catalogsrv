package apis

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

func deleteObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

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

	if workspace != "" {
		kind = types.WorkspaceKind
	} else if variantName != "" {
		kind = types.VariantKind
	} else if catalogName != "" {
		kind = types.CatalogKind
	} else if namespace != "" {
		kind = types.NamespaceKind
	}

	rm, err := catalogmanager.ResourceManagerFromName(ctx, kind, n)
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
