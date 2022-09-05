package main

import (
	"log"
	_ "net/http/pprof"

	_ "a.yandex-team.ru/yt/go/docker_registry/internal/utils"
	_ "a.yandex-team.ru/yt/go/docker_registry/internal/ytauth"
	_ "a.yandex-team.ru/yt/go/docker_registry/internal/ytdriver"
	_ "a.yandex-team.ru/yt/go/docker_registry/internal/ytrepositorymiddleware"
	"github.com/distribution/distribution/v3/registry"
	_ "github.com/distribution/distribution/v3/registry/auth/htpasswd"
	_ "github.com/distribution/distribution/v3/registry/auth/token"
	_ "github.com/distribution/distribution/v3/registry/proxy"
)

func main() {
	if err := registry.RootCmd.Execute(); err != nil {
		log.Fatalf("run failed with error: %v", err)
	}
}
