package migrate

import (
	"context"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// Create creates new dynamic table with provided schema.
func Create(ctx context.Context, yc yt.Client, path ypath.Path, schema schema.Schema) error {
	_, err := yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]any{
			"dynamic": true,
			"schema":  schema,
		},
	})

	return err
}
