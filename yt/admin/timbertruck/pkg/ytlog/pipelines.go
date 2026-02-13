package ytlog

import (
	"context"
	"time"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
)

const (
	// BufferSizeReserve is the extra space added to TextFileLineLimit for buffer size.
	BufferSizeReserve = 4 * 1024 * 1024
)

type BaseLogPipelineOptions struct {
	AntisecretTransform    pipelines.Transform[[]byte, []byte]
	QueueBatchSize         int
	QueueBatchFlushTimeout time.Duration
	TextFileLineLimit      int
}

type TextLogPipelineOptions struct {
	BaseLogPipelineOptions

	Cluster    string
	TskvFormat string
}

func NewTextLogPipeline(task timbertruck.TaskArgs, output pipelines.Output[pipelines.Row], options TextLogPipelineOptions) (p *pipelines.Pipeline, err error) {
	if options.Cluster == "" {
		panic("Cluster is not specified")
	}
	if options.TskvFormat == "" {
		panic("TskvFormat is not specified")
	}
	p, lineInfos, err := pipelines.NewTextPipeline(task.Controller.Logger(), task.Path, task.Position, pipelines.TextPipelineOptions{
		LineLimit:      options.TextFileLineLimit,
		BufferLimit:    options.TextFileLineLimit + BufferSizeReserve,
		OnTruncatedRow: task.Controller.OnSkippedRow,
	})
	if err != nil {
		return
	}

	notEmptyLineInfos := pipelines.Apply(pipelines.NewDiscardEmptyLinesTransform(task.Controller.Logger()), lineInfos)
	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), notEmptyLineInfos)
	parsed := pipelines.Apply(NewParseLineTransform(task.Controller.Logger(), task.Controller.OnSkippedRow), lines)
	tskv := pipelines.Apply(NewTskvLineTransform(options.Cluster, options.TskvFormat), parsed)
	batched := pipelines.Apply(NewBatchLinesTransform(options.QueueBatchSize, options.QueueBatchFlushTimeout), tskv)
	if options.AntisecretTransform != nil {
		batched = pipelines.Apply(options.AntisecretTransform, batched)
	}

	rows := pipelines.ApplyFunc(func(ctx context.Context, meta pipelines.RowMeta, value []byte, emit pipelines.EmitFunc[pipelines.Row]) {
		emit(ctx, meta, pipelines.Row{
			Payload: value,
			SeqNo:   meta.End.LogicalOffset,
		})
	}, batched)

	pipelines.ApplyOutput(output, rows)
	return
}

type JSONLogPipelineOptions struct {
	BaseLogPipelineOptions
}

func NewJSONLogPipeline(task timbertruck.TaskArgs, output pipelines.Output[pipelines.Row], options JSONLogPipelineOptions) (p *pipelines.Pipeline, err error) {
	p, lineInfos, err := pipelines.NewTextPipeline(task.Controller.Logger(), task.Path, task.Position, pipelines.TextPipelineOptions{
		LineLimit:      options.TextFileLineLimit,
		BufferLimit:    options.TextFileLineLimit + BufferSizeReserve,
		OnTruncatedRow: task.Controller.OnSkippedRow,
	})
	if err != nil {
		return
	}

	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), lineInfos)
	lines = pipelines.Apply(NewValidateJSONTransform(task.Controller.Logger(), task.Controller.OnSkippedRow), lines)
	batched := pipelines.Apply(NewBatchLinesTransform(options.QueueBatchSize, options.QueueBatchFlushTimeout), lines)
	if options.AntisecretTransform != nil {
		batched = pipelines.Apply(options.AntisecretTransform, batched)
	}

	rows := pipelines.ApplyFunc(func(ctx context.Context, meta pipelines.RowMeta, value []byte, emit pipelines.EmitFunc[pipelines.Row]) {
		emit(ctx, meta, pipelines.Row{
			Payload: value,
			SeqNo:   meta.End.LogicalOffset,
		})
	}, batched)

	pipelines.ApplyOutput(output, rows)
	return
}

type YSONLogPipelineOptions struct {
	BaseLogPipelineOptions
}

func NewYSONLogPipeline(task timbertruck.TaskArgs, output pipelines.Output[pipelines.Row], options YSONLogPipelineOptions) (p *pipelines.Pipeline, err error) {
	p, lineInfos, err := pipelines.NewTextPipeline(task.Controller.Logger(), task.Path, task.Position, pipelines.TextPipelineOptions{
		LineLimit:      options.TextFileLineLimit,
		BufferLimit:    options.TextFileLineLimit + BufferSizeReserve,
		OnTruncatedRow: task.Controller.OnSkippedRow,
	})
	if err != nil {
		return
	}

	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), lineInfos)
	lines = pipelines.Apply(NewValidateYSONTransform(task.Controller.Logger(), task.Controller.OnSkippedRow), lines)
	batched := pipelines.Apply(NewBatchLinesTransform(options.QueueBatchSize, options.QueueBatchFlushTimeout), lines)
	if options.AntisecretTransform != nil {
		batched = pipelines.Apply(options.AntisecretTransform, batched)
	}

	rows := pipelines.ApplyFunc(func(ctx context.Context, meta pipelines.RowMeta, value []byte, emit pipelines.EmitFunc[pipelines.Row]) {
		emit(ctx, meta, pipelines.Row{
			Payload: value,
			SeqNo:   meta.End.LogicalOffset,
		})
	}, batched)

	pipelines.ApplyOutput(output, rows)
	return
}
