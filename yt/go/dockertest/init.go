package dockertest

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/testcontainers/testcontainers-go"
)

func InitYTsaurusContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*YTsaurusContainter, error) {
	volume := os.Getenv("YT_VOLUME")
	if volume != "" {
		opts = append(opts, WithVolumeMount(volume))
	}

	c, err := RunContainer(ctx, opts...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if err := c.Terminate(ctx); err != nil {
				log.Fatalf("failed to terminate container: %v", err)
			}
		}
	}()

	if err = os.Setenv("YT_PROXY", c.Proxy); err != nil {
		return nil, fmt.Errorf("failed to set YT_PROXY: %w", err)
	}

	if err = os.Setenv("YT_ID", c.Cluster); err != nil {
		return nil, fmt.Errorf("failed to set YT_ID: %w", err)
	}

	return c, nil
}
