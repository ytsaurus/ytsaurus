package yt

import (
	"net/http"
)

type badRequestError struct {
	error
}

type internalServerError struct {
	error
}

type authenticationError struct {
	error
}

func (badRequestError) StatusBadRequest(r *http.Request, w http.ResponseWriter) {
	w.WriteHeader(http.StatusBadRequest)
}

func (internalServerError) SetHeaders(r *http.Request, w http.ResponseWriter) {
	w.WriteHeader(http.StatusInternalServerError)
}

func (authenticationError) SetHeaders(r *http.Request, w http.ResponseWriter) {
	w.WriteHeader(http.StatusForbidden)
}
