package apierror

import (
	"errors"
	"net/http"

	"gorm.io/gorm"
)

var (
	ErrUnauthorized = errors.New("unauthorized")
	ErrValidation   = errors.New("validation error")
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
)

type classifiedError struct {
	kind    error
	message string
}

func (e classifiedError) Error() string { return e.message }
func (e classifiedError) Unwrap() error { return e.kind }

func New(kind error, message string) error {
	return classifiedError{kind: kind, message: message}
}

func ClassifyHTTPStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrValidation):
		return http.StatusBadRequest
	case errors.Is(err, ErrNotFound), errors.Is(err, gorm.ErrRecordNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
