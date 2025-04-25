package apis

import (
	"net/http"

	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

func deleteObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()

	n, err := getResourceName(r)
	if err != nil {
		return nil, err
	}
	kind := getResourceKind(r)
	if kind == types.InvalidKind {
		return nil, httpx.ErrInvalidRequest()
	}

	rm, err := catalogmanager.ResourceManagerForKind(ctx, kind, n)
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
