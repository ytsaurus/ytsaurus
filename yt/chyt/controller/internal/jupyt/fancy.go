package jupyt

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func appendArtifactDescription(ctx context.Context, description *map[string]yson.RawValue, ytc yt.Client, name string, path ypath.Path) (err error) {
	var artifactAttrs yson.RawValue
	err = ytc.GetNode(ctx, path.Attr("user_attributes"), &artifactAttrs, nil)
	if err != nil {
		return
	}
	(*description)[name] = artifactAttrs
	return
}

func buildDescription(cluster string, alias string, enableYandexSpecificLinks bool) map[string]any {
	return map[string]any{}
}
