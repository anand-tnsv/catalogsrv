package dberror

import (
	"fmt"
	"strings"

	"github.com/mugiliam/common/apperrors"
)

// dbError implements the apperrors.Error interface
type dbError struct {
	msg string
	err error
}

func (e *dbError) Error() string {
	return e.msg
}

func (e *dbError) Unwrap() error {
	return e.err
}

func (e *dbError) Msg(msg string) apperrors.Error {
	return &dbError{
		msg: msg,
		err: e,
	}
}

func (e *dbError) MsgErr(msg string, err ...error) apperrors.Error {
	f := ""
	if e.err != nil {
		f = "%w "
	}
	for _, e := range err {
		_ = e
		f = f + "%w "
	}
	// trim the trailing space
	f = strings.TrimRight(f, " ")
	return &dbError{
		msg: msg,
		err: fmt.Errorf(f, e.Err, err),
	}
}

func (e *dbError) Err(err ...error) apperrors.Error {
	f := ""
	if e.err != nil {
		f = "%w "
	}
	for _, e := range err {
		_ = e
		f = f + "%w "
	}
	// trim the trailing space
	f = strings.TrimRight(f, " ")
	return &dbError{
		msg: e.msg,
		err: fmt.Errorf(f, e, err),
	}
}

func New(msg string) *dbError {
	return &dbError{
		msg: msg,
		err: nil,
	}
}

var (
	ErrDatabase       apperrors.Error = New("db error")
	ErrAlreadyExists  apperrors.Error = ErrDatabase.Msg("already exists")
	ErrNotFound       apperrors.Error = ErrDatabase.Msg("not found")
	ErrInvalidInput   apperrors.Error = ErrDatabase.Msg("invalid input")
	ErrInvalidCatalog apperrors.Error = ErrDatabase.Msg("invalid catalog")
)
