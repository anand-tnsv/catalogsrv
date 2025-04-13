package apis

import (
	"net/http"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/common/httpx"
)

func ToHttpxError(err error) error {
	if appErr, ok := err.(apperrors.Error); ok {
		statusCode := appErr.StatusCode()
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		return &httpx.Error{
			StatusCode:  statusCode,
			Description: appErr.ErrorAll(),
		}
	}
	return err
}
