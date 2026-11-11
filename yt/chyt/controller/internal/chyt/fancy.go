package chyt

import (
	"context"
	"net/url"

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

func buildDescription(cluster string, alias string, enableYandexSpecificLinks bool) map[string]any {
	if enableYandexSpecificLinks {
		return map[string]any{
			"clique_url": strawberry.ToYsonURL(
				"https://yt.yandex-team.ru/" + cluster + "/chyt/" + url.PathEscape(alias)),
			"yql_url": strawberry.ToYsonURL(
				"https://yql.yandex-team.ru/?query=use%20chyt." + cluster + "/" + alias +
					"%3B%0A%0Aselect%201%3B&query_type=CLICKHOUSE"),
			"monium_root_url": strawberry.ToYsonURL(
				"https://m.yandex-team.ru/projects/yt/explorer/queries?q.0.text=" +
					url.QueryEscape("{project=yt,cluster="+cluster+",service=clickhouse,operation_alias="+alias+"}")),
			"monium_dashboard_url": strawberry.ToYsonURL(
				"https://m.yandex-team.ru/projects/yt/dashboards/chyt_v2" +
					"?p[service]=clickhouse&p[cluster]=" + cluster + "&p[operation_alias]=" + url.QueryEscape(alias)),
		}
	} else {
		return map[string]any{}
	}
}
