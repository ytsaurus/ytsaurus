package certs

import (
	"go.ytsaurus.tech/library/go/core/resource"
)

func InternalCAs() []byte {
	return resource.Get("/certifi/internal.pem")
}

func CommonCAs() []byte {
	return resource.Get("/certifi/common.pem")
}

func InternalYCKZCAs() []byte {
	return resource.Get("/certifi/internalYCKZ.pem")
}
