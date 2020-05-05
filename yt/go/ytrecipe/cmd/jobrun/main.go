package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"a.yandex-team.ru/library/go/core/log"
	zaplog "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/library/go/yandex/oauth"
	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/ytrecipe"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/secret"
)

const (
	ytCLIApplicationID = "322d0081ab604f2f89517dc87ee978f8"
	ytCLISecret        = "d5089c88468c4bdfac52c7bda177d04f"
)

func do() error {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{yatest.OutputPath("jobrun.log")}
	cfg.Level.SetLevel(zapcore.DebugLevel)

	l, err := zaplog.New(cfg)
	if err != nil {
		return err
	}

	configYS, err := ioutil.ReadFile(os.Getenv("YTRECIPE_CONFIG_PATH"))
	if err != nil {
		return err
	}

	config := ytrecipe.DefaultConfig
	if err := yson.Unmarshal(configYS, &config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	ytToken, err := secret.GetYTToken()
	if err != nil {
		l.Warn("secret YT_TOKEN is not available", log.Error(err))

		ytToken, err = oauth.GetTokenBySSH(context.Background(), ytCLIApplicationID, ytCLISecret)
		if err != nil {
			return fmt.Errorf("failed to get YT token from ssh: %w", err)
		}
	}

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:  config.Cluster,
		Logger: l,
		Token:  ytToken,
	})
	if err != nil {
		return err
	}

	r := &ytrecipe.Runner{
		Config: &config,
		YT:     yc,
		L:      l,
	}

	return r.RunJob()
}

func main() {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	if err := do(); err != nil {
		fmt.Fprintf(os.Stderr, "jobrun failed: %+v", yterrors.FromError(err))
		os.Exit(1)
	}
}
