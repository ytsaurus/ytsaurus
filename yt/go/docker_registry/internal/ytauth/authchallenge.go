package yt

import (
	"net/http"
)

type authChallenge struct {
	error
}

func (c *authChallenge) challengeParams(r *http.Request) string {
	return "Basic"
}

func (c *authChallenge) SetHeaders(r *http.Request, w http.ResponseWriter) {
	w.Header().Add("WWW-Authenticate", c.challengeParams(r))
}
