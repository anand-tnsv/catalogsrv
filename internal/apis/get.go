package apis

import (
	"net/http"

	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

func getObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	n, err := getResourceName(r)
	if err != nil {
		return nil, err
	}

	kind = getResourceKind(r)
	if kind == types.InvalidKind {
		return nil, httpx.ErrInvalidRequest()
	}

	rm, err := catalogmanager.ResourceManagerForKind(ctx, kind, n)
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
