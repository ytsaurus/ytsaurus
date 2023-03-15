package main

import (
	"context"

	"github.com/spf13/pflag"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/fetcher"
)

var (
	flagPath   string
	flagProxy  string
	flagSystem string
)

func main() {
	pflag.StringVar(&flagPath, "path", "//home/ytprof", "path to ytprof")
	pflag.StringVar(&flagProxy, "proxy", "hahn", "storage proxy")
	pflag.StringVar(&flagSystem, "system", "", "storage proxy")
	pflag.Parse()

	storagePath := ypath.Path(flagPath).Child(ytprof.DirStorage)
	configPath := ypath.Path(flagPath).Child(ytprof.DirConfigs)

	l := ytlog.Must()

	l.Debug("starting to create system",
		log.String("system", flagSystem),
		log.String("proxy", flagProxy),
		log.String("path", flagPath),
	)

	YT, err := ythttp.NewClient(&yt.Config{
		Proxy:             flagProxy,
		ReadTokenFromFile: true,
	})
	if err != nil {
		l.Fatal("yt client not created", log.Error(err))
	}
	l.Debug("yt client created")

	ctx := context.Background()

	fullStoragePath := storagePath.Child(flagSystem)
	_, err = YT.CreateNode(
		ctx,
		fullStoragePath,
		yt.NodeMap,
		&yt.CreateNodeOptions{},
	)
	if err != nil {
		l.Fatal("map node not created", log.Error(err))
	}

	fullConfigPath := configPath.Child(flagSystem)
	_, err = YT.CreateNode(
		ctx,
		fullConfigPath,
		yt.NodeMap,
		&yt.CreateNodeOptions{},
	)
	if err != nil {
		l.Fatal("map node not created", log.Error(err))
	}
	l.Debug("map nodes created")

	for table, description := range ytprof.Tables {
		err = migrate.Create(ctx, YT, fullStoragePath.Child(table), description.Schema)
		if err != nil {
			l.Fatal("table creation failed", log.Error(err), log.String("table", table))
		}

		if _, ok := description.Attributes["atomicity"]; ok {
			err = YT.SetNode(
				ctx,
				fullStoragePath.Child(table).Attr("atomicity"),
				description.Attributes["atomicity"],
				&yt.SetNodeOptions{},
			)
			if err != nil {
				l.Fatal("setting atomicity failed", log.Error(err), log.String("table", table))
			}
		}
	}
	l.Debug("all tables created")

	ys, err := yson.MarshalFormat(fetcher.Configs{}, yson.FormatPretty)
	if err != nil {
		l.Fatal("marshalling empty config failed")
	}

	_, err = YT.CreateNode(ctx, fullConfigPath.Child(ytprof.ObjectConfig), yt.NodeDocument, &yt.CreateNodeOptions{
		Attributes: map[string]interface{}{
			"value": yson.RawValue(ys),
		},
	})
	if err != nil {
		l.Fatal("creation of config document failed")
	}

	l.Info("success")
}
