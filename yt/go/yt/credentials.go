package yt

import "net/http"

type Credentials interface {
	Set(r *http.Request)
}

type TokenCredentials struct {
	Token string
}

func (c *TokenCredentials) Set(r *http.Request) {
	r.Header.Add("Authorization", "OAuth "+c.Token)
}
