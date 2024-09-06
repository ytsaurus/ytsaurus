package ytwalk_test

import (
	"context"
	"log"
	"os"
	"testing"

	"go.ytsaurus.tech/yt/go/dockertest"
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := dockertest.InitYTsaurusContainer(ctx)
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
