package main

import (
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/app"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/ytlog"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/ytqueue"
)

type Config struct {
	app.Config `yaml:",inline"`

	YtTokenFile string          `yaml:"yt_token_file"`
	JSONLogs    []JSONLogConfig `yaml:"json_logs"`
}

type JSONLogConfig struct {
	timbertruck.StreamConfig `yaml:",inline"`

	LogbrokerTopic string `yaml:"logbroker_topic"`

	YtQueue []ytqueue.Config `yaml:"yt_queue"`
}

func sessionID(hostname, filepath string) string {
	return fmt.Sprintf("%v:%v", hostname, filepath)
}

func newOutput(config *Config, logConfig JSONLogConfig, task timbertruck.TaskArgs) (output pipelines.Output[pipelines.Row], err error) {
	ctx := task.Context
	var outputList []pipelines.Output[pipelines.Row]

	sessionID := sessionID(config.Hostname, task.Path)

	var ytToken string
	if config.YtTokenFile != "" {
		var ytTokenBytes []byte
		ytTokenBytes, err = os.ReadFile(config.YtTokenFile)
		if err != nil {
			return
		}
		ytToken = string(ytTokenBytes)
	}

	if logConfig.YtQueue != nil {
		for _, ytQueueConfig := range logConfig.YtQueue {
			ytConfig := ytqueue.OutputConfig{
				Cluster:          ytQueueConfig.Cluster,
				QueuePath:        ytQueueConfig.QueuePath,
				ProducerPath:     ytQueueConfig.ProducerPath,
				RPCProxyRole:     ytQueueConfig.RPCProxyRole,
				CompressionCodec: ytQueueConfig.CompressionCodec,
				SessionID:        sessionID,
				Token:            ytToken,
				Logger:           task.Controller.Logger(),
			}

			var ytOutput pipelines.Output[pipelines.Row]
			ytOutput, err = ytqueue.NewOutput(ctx, ytConfig)
			if err != nil {
				return
			}
			outputList = append(outputList, ytOutput)
		}
	}

	if len(outputList) == 0 {
		panic(fmt.Sprintf("no output configured for stream %v", logConfig))
	}
	output = pipelines.NewMultiOutput(outputList...)
	return
}

func main() {
	app, config := app.MustNewApp[Config]()
	defer func() {
		err := recover()
		_ = app.Close() // flush timbertruck's log.
		if err != nil {
			panic(err)
		}
	}()

	for _, jsonLogConfig := range config.JSONLogs {
		app.AddStream(jsonLogConfig.StreamConfig, func(task timbertruck.TaskArgs) (p *pipelines.Pipeline, err error) {
			output, err := newOutput(config, jsonLogConfig, task)
			if err != nil {
				return
			}
			p, err = ytlog.NewJSONLogPipeline(task, output, ytlog.JSONLogPipelineOptions{})
			return
		})
	}

	err := app.Run()
	if err != nil {
		app.Fatalf("%v", err)
	}
}
