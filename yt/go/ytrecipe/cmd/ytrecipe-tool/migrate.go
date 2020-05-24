package main

import (
	"context"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/blobcache"
)

func init() {
	rootCmd.AddCommand(migrateCmd)

	migrateCmd.Flags().StringVar(&flagRoot, "root", "", "")
}

var (
	flagRoot string
)

var migrateCmd = &cobra.Command{
	Use: "migrate",
	Run: wrapRun(doMigrate),
}

func doMigrate() error {
	yc, err := ythttp.NewClientCli(flagProxy)
	if err != nil {
		return err
	}

	config := blobcache.Config{
		Root: ypath.Path(flagRoot),
	}

	cache := blobcache.NewCache(&nop.Logger{}, yc, config)
	return cache.Migrate(context.Background())
}
