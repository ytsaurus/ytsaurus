package integration

import (
	"context"
	"log"
	"os"
	"testing"

	"go.ytsaurus.tech/yt/go/dockertest"
	"go.ytsaurus.tech/yt/go/mapreduce"
)

func TestMain(m *testing.M) {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := dockertest.InitYTsaurusContainer(
		ctx,
		dockertest.WithDynamicTables(),
		dockertest.WithRPCProxies(1),
		dockertest.WithDiscoveryServers(1),
		dockertest.WithOperationsArchive(),
		// Enable signing of distributed write sessions on the HTTP proxy.
		dockertest.WithProxyConfigPatch(map[string]any{
			"signature_components": map[string]any{
				"validation": map[string]any{
					"cypress_key_reader": map[string]any{},
				},
				"generation": map[string]any{
					"cypress_key_writer": map[string]any{},
					"generator":          map[string]any{},
					"key_rotator":        map[string]any{},
				},
			},
		}),
	)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}
	defer func() {
		if err := c.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %v", err)
		}
	}()

	return m.Run()
}
