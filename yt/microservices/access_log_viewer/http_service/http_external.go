//go:build !internal
// +build !internal

package main

import (
	"net/http"

	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
)

func corsHandler(conf *ytmsvc.CORSConfig) func(next http.Handler) http.Handler {
	return ytmsvc.CORS(conf)
}
