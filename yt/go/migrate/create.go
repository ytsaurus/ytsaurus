package migrate

import (
	"context"

	"a.yandex-team.ru/yt/go/ypath"

	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/yt"
)

// Create creates new dynamic table with provided schema.
func Create(ctx context.Context, yc yt.Client, path ypath.Path, schema schema.Schema) error {
	_, err := yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Recursive: true,
		Attributes: map[string]interface{}{
			"dynamic": true,
			"schema":  schema,
		},
	})

	return err
}
