//go:build !internal
// +build !internal

package main

import (
	"net/http"

	lib "go.ytsaurus.tech/yt/microservices/lib/go"
)

func corsHandler(conf *lib.CORSConfig) func(next http.Handler) http.Handler {
	return lib.CORS(conf)
}
