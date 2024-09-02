package ytlog

import (
	"context"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
)

const TextFileBufferLimit = 20 * 1024 * 1024
const TextFileLineLimit = 16 * 1024 * 1024
const QueueBatchSize = 16 * 1024 * 1024

type TextLogPipelineOptions struct {
	Cluster    string
	TskvFormat string

	AntisecretTransform pipelines.Transform[[]byte, []byte]
}

func NewTextLogPipeline(task timbertruck.TaskArgs, output pipelines.Output[pipelines.Row], options TextLogPipelineOptions) (p *pipelines.Pipeline, err error) {
	if options.Cluster == "" {
		panic("Cluster is not specified")
	}
	if options.TskvFormat == "" {
		panic("TskvFormat is not specified")
	}
	p, lineInfos, err := pipelines.NewTextPipeline(task.Path, task.Position, pipelines.TextPipelineOptions{
		LineLimit:   TextFileLineLimit,
		BufferLimit: TextFileBufferLimit,
	})
	if err != nil {
		return
	}

	notEmptyLineInfos := pipelines.Apply(pipelines.NewDiscardEmptyLinesTransform(task.Controller.Logger()), lineInfos)
	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), notEmptyLineInfos)
	parsed := pipelines.Apply(NewParseLineTransform(task.Controller.Logger()), lines)
	tskv := pipelines.Apply(NewTskvTransform(QueueBatchSize, options.Cluster, options.TskvFormat), parsed)
	if options.AntisecretTransform != nil {
		tskv = pipelines.Apply(options.AntisecretTransform, tskv)
	}

	rows := pipelines.ApplyFunc(func(ctx context.Context, meta pipelines.RowMeta, tskv []byte, emit pipelines.EmitFunc[pipelines.Row]) {
		emit(ctx, meta, pipelines.Row{
			Payload: tskv,
			SeqNo:   meta.End.LogicalOffset,
		})
		task.Controller.NotifyProgress(meta.End)
	}, tskv)

	pipelines.ApplyOutput(output, rows)
	return
}

type JSONLogPipelineOptions struct {
	AntisecretTransform pipelines.Transform[[]byte, []byte]
}

func NewJSONLogPipeline(task timbertruck.TaskArgs, output pipelines.Output[pipelines.Row], options JSONLogPipelineOptions) (p *pipelines.Pipeline, err error) {
	p, lineInfos, err := pipelines.NewTextPipeline(task.Path, task.Position, pipelines.TextPipelineOptions{
		LineLimit:   TextFileLineLimit,
		BufferLimit: TextFileBufferLimit,
	})
	if err != nil {
		return
	}

	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), lineInfos)
	lines = pipelines.Apply(NewValidateJSONTransform(task.Controller.Logger()), lines)
	batched := pipelines.Apply(NewBatchLinesTransform(QueueBatchSize), lines)
	if options.AntisecretTransform != nil {
		batched = pipelines.Apply(options.AntisecretTransform, batched)
	}

	rows := pipelines.ApplyFunc(func(ctx context.Context, meta pipelines.RowMeta, tskv []byte, emit pipelines.EmitFunc[pipelines.Row]) {
		emit(ctx, meta, pipelines.Row{
			Payload: []byte(tskv),
			SeqNo:   meta.End.LogicalOffset,
		})
		task.Controller.NotifyProgress(meta.End)
	}, batched)

	pipelines.ApplyOutput(output, rows)
	return
}
