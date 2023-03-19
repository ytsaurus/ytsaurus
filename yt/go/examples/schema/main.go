package main

import (
	"context"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

func main() {
	ctx := context.Background()

	yc, _ := ythttp.NewClient(&yt.Config{Proxy: "freud"})

	// Creating table with a schema.
	tablePath := ypath.Path("//tmp/table-schema-example")
	tableSchema := schema.MustInfer(struct {
		UUID  int64  `yson:"uuid"`
		Login string `yson:"login"`
	}{})

	_, _ = yt.CreateTable(ctx, yc, tablePath, yt.WithSchema(tableSchema))

	// Getting schema of an existing table.
	_ = yc.GetNode(ctx, tablePath.Attr("schema"), &tableSchema, nil)

	// Altering schema of an existing table.
	newSchema := schema.MustInfer(struct {
		UUID  int64   `yson:"uuid"`
		Login string  `yson:"login"`
		Name  *string `yson:"name"`
	}{})

	_ = yc.AlterTable(ctx, tablePath, &yt.AlterTableOptions{Schema: &newSchema})
}
