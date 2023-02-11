package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/ytrecipe"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/ytexec"
)

func do() error {
	configYS, err := os.ReadFile(os.Getenv("YTRECIPE_CONFIG_PATH"))
	if err != nil {
		return err
	}

	config := ytrecipe.DefaultConfig
	if err := yson.Unmarshal(configYS, &config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	var execConfig ytexec.Config
	execConfig.Exec = ytexec.ExecConfig{
		JobLog:         yatest.OutputPath("job.log"),
		ExecLog:        yatest.OutputPath("ytexec.log"),
		DmesgLog:       yatest.OutputPath("dmesg.log"),
		ReadmeFile:     yatest.OutputPath("README.md"),
		DownloadScript: yatest.OutputPath("download.sh"),
		PreparedFile:   yatest.OutputPath("prepare.json"),
		ResultFile:     yatest.OutputPath("result.json"),
	}

	if os.Getenv("YT_TOKEN") != "" {
		execConfig.Exec.YTTokenEnv = "YT_TOKEN"
	}

	env, err := ytrecipe.CaptureEnv()
	if err != nil {
		return err
	}

	env.FillConfig(&execConfig)
	if err := config.FillConfig(&execConfig); err != nil {
		return err
	}

	configJS, err := json.MarshalIndent(execConfig, "", "    ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(yatest.OutputPath("ytexec.json"), configJS, 0666); err != nil {
		return err
	}

	exec, err := ytexec.New(execConfig)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := exec.Run(ctx); err != nil {
		return err
	}

	exitRow, err := exec.ReadOutputs(ctx)
	if err != nil {
		return err
	}

	if exitRow.KilledBySignal != 0 {
		// If job was killed by internal timeout, wait for corresponding signal from out parent.
		// Otherwise process will exit too early and CRASH/TIMEOUT statuses get confused.

		ch := make(chan os.Signal, 1)
		signal.Notify(ch, exitRow.KilledBySignal)
		<-ch
	}

	os.Exit(exitRow.ExitCode)
	return nil
}

func main() {
	if ytexec.IsMkdir() {
		os.Exit(ytexec.Mkdir())
	}

	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	if err := do(); err != nil {
		fmt.Fprintf(os.Stderr, "jobrun failed: %+v", yterrors.FromError(err))
		os.Exit(1)
	}
}
