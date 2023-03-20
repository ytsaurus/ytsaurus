package chyt

import (
	"context"

	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
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

func buildDescription(cluster string, alias string, enableYandexSpecificLinks bool) map[string]interface{} {
	if enableYandexSpecificLinks {
		return map[string]interface{}{
			"yql_url": strawberry.ToYsonURL(
				"https://yql.yandex-team.ru/?query=use%20chyt." + cluster + "/" + alias +
					"%3B%0A%0Aselect%201%3B&query_type=CLICKHOUSE"),
			"solomon_root_url": strawberry.ToYsonURL(
				"https://solomon.yandex-team.ru/?project=yt&cluster=" + cluster + "&service=clickhouse&operation_alias=" +
					alias),
			"solomon_dashboard_url": strawberry.ToYsonURL(
				"https://solomon.yandex-team.ru/?project=yt&cluster=" + cluster + "&service=clickhouse&cookie=Aggr" +
					"&dashboard=chyt_v2&l.operation_alias=" + alias),
		}
	} else {
		return map[string]interface{}{}
	}
}
