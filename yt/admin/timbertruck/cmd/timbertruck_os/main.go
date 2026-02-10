package main

import (
	"fmt"
	"os"
	"time"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/app"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/ytlog"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/ytqueue"
)

const (
	DefaultTextFileLineLimit = 16 * 1024 * 1024
	DefaultQueueBatchSize    = 16 * 1024 * 1024
)

type Config struct {
	app.Config `yaml:",inline"`

	YtTokenFile string          `yaml:"yt_token_file"`
	JSONLogs    []JSONLogConfig `yaml:"json_logs"`
}

func (c *Config) SetDefaults() {
	for i := range c.JSONLogs {
		c.JSONLogs[i].SetDefaults()
	}
}

type JSONLogConfig struct {
	timbertruck.StreamConfig `yaml:",inline"`

	// QueueBatchSize is the buffer size at which a flush to the output is triggered.
	// Lines larger than QueueBatchSize will be flushed individually.
	//
	// Default value is 16777216 (16 MiB).
	QueueBatchSize int `yaml:"queue_batch_size"`

	// QueueBatchFlushTimeout defines maximum time to keep a partially filled buffer before flushing.
	// If 0, flush only when buffer reaches QueueBatchSize or on file completion.
	//
	// Default value is 0 (disabled).
	QueueBatchFlushTimeout time.Duration `yaml:"queue_batch_flush_timeout"`

	// TextFileLineLimit specifies the maximum allowed length of a line in the text file.
	// Lines longer than this value will be truncated.
	//
	// Default value is 16777216 (16 MiB).
	TextFileLineLimit int `yaml:"text_file_line_limit"`

	YtQueue []ytqueue.Config `yaml:"yt_queue"`
}

func (c *JSONLogConfig) SetDefaults() {
	if c.TextFileLineLimit == 0 {
		c.TextFileLineLimit = DefaultTextFileLineLimit
	}
	if c.QueueBatchSize == 0 {
		c.QueueBatchSize = DefaultQueueBatchSize
	}
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
				Cluster:               ytQueueConfig.Cluster,
				QueuePath:             ytQueueConfig.QueuePath,
				ProducerPath:          ytQueueConfig.ProducerPath,
				RPCProxyRole:          ytQueueConfig.RPCProxyRole,
				CompressionCodec:      ytQueueConfig.CompressionCodec,
				SessionID:             sessionID,
				Token:                 ytToken,
				Logger:                task.Controller.Logger(),
				BytesPerRow:           logConfig.QueueBatchSize,
				BytesPerRowsBatch:     ytQueueConfig.BytesPerRowsBatch,
				RowsBatchFlushTimeout: ytQueueConfig.RowsBatchFlushTimeout,
				MaxCompressedRowBytes: ytQueueConfig.MaxCompressedRowBytes,
				OnSent: func(meta pipelines.RowMeta) {
					task.Controller.NotifyProgress(meta.End)
				},
				OnSkippedRow: task.Controller.OnSkippedRow,
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
	config.SetDefaults()

	for _, jsonLogConfig := range config.JSONLogs {
		app.AddStream(jsonLogConfig.StreamConfig, func(task timbertruck.TaskArgs) (p *pipelines.Pipeline, err error) {
			output, err := newOutput(config, jsonLogConfig, task)
			if err != nil {
				return
			}
			p, err = ytlog.NewJSONLogPipeline(task, output, ytlog.JSONLogPipelineOptions{
				BaseLogPipelineOptions: ytlog.BaseLogPipelineOptions{
					QueueBatchSize:         jsonLogConfig.QueueBatchSize,
					QueueBatchFlushTimeout: jsonLogConfig.QueueBatchFlushTimeout,
					TextFileLineLimit:      jsonLogConfig.TextFileLineLimit,
				}})
			return
		})
	}

	err := app.Run()
	if err != nil {
		app.Fatalf("%v", err)
	}
}
