package main

import (
	"context"

	"github.com/spf13/pflag"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof"
)

var (
	flagPath   string
	flagProxy  string
	flagSystem string
)

func main() {
	pflag.StringVar(&flagPath, "path", "//home/ytprof/storage", "path to storage")
	pflag.StringVar(&flagProxy, "proxy", "hahn", "storage proxy")
	pflag.StringVar(&flagSystem, "system", "", "storage proxy")
	pflag.Parse()

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

	fullPath := ypath.Path(flagPath).Child(flagSystem)
	_, err = YT.CreateNode(
		ctx,
		fullPath,
		yt.NodeMap,
		&yt.CreateNodeOptions{},
	)
	if err != nil {
		l.Fatal("map node not created", log.Error(err))
	}
	l.Debug("map node created")

	err = migrate.Create(ctx, YT, fullPath.Child(ytprof.TableData), ytprof.SchemaData)
	if err != nil {
		l.Fatal("table creation failed", log.Error(err), log.String("table", ytprof.TableData))
	}

	err = migrate.Create(ctx, YT, fullPath.Child(ytprof.TableMetadata), ytprof.SchemaMetadata)
	if err != nil {
		l.Fatal("table creation failed", log.Error(err), log.String("table", ytprof.TableMetadata))
	}

	err = migrate.Create(ctx, YT, fullPath.Child(ytprof.TableMetadataTags), ytprof.SchemaMetadataTags)
	if err != nil {
		l.Fatal("table creation failed", log.Error(err), log.String("table", ytprof.TableData))
	}

	err = migrate.Create(ctx, YT, fullPath.Child(ytprof.TableMetadataTagsValues), ytprof.SchemaMetadataTagsValues)
	if err != nil {
		l.Fatal("table creation failed", log.Error(err), log.String("table", ytprof.TableMetadataTagsValues))
	}
	l.Debug("all tables created")

	err = YT.SetNode(ctx, fullPath.Child(ytprof.TableMetadataTags).Attr("atomicity"), "none", &yt.SetNodeOptions{})
	if err != nil {
		l.Fatal("setting atomicity failed", log.Error(err), log.String("table", ytprof.TableMetadataTags))
	}

	err = YT.SetNode(ctx, fullPath.Child(ytprof.TableMetadataTagsValues).Attr("atomicity"), "none", &yt.SetNodeOptions{})
	if err != nil {
		l.Fatal("setting atomicity failed", log.Error(err), log.String("table", ytprof.TableMetadataTagsValues))
	}

	l.Info("success")
}
