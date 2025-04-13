package apis

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
)

func getCatalog(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	catalogName := chi.URLParam(r, "catalogName")
	cm, err := catalogmanager.LoadCatalogManagerByName(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	j, err := cm.ToJson(ctx)
	if err != nil {
		return nil, err
	}
	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   j,
	}
	return rsp, nil
}
