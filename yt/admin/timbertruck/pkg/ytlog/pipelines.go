package ytlog

import (
	"context"
	"fmt"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/timbertruck"
)

const TextFileBufferLimit = 20 * 1024 * 1024
const DefaultTextFileLineLimit = 16 * 1024 * 1024
const DefaultQueueBatchSize = 16 * 1024 * 1024

type BaseLogPipelineOptions struct {
	AntisecretTransform pipelines.Transform[[]byte, []byte]
	QueueBatchSize      int
	TextFileLineLimit   int
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
	textFileLineLimit := DefaultTextFileLineLimit
	if options.TextFileLineLimit > 0 {
		textFileLineLimit = options.TextFileLineLimit
	}
	queueBatchSize := DefaultQueueBatchSize
	if options.QueueBatchSize > 0 {
		queueBatchSize = options.QueueBatchSize
	}
	if queueBatchSize < textFileLineLimit {
		panic(fmt.Sprintf("bad options %v; queueBatchSize (%d) MUST BE >= textFileLineLimit (%d)", options, queueBatchSize, textFileLineLimit))
	}
	p, lineInfos, err := pipelines.NewTextPipeline(
		task.Controller.Logger().With("stagedpath", task.Path), task.Path, task.Position, pipelines.TextPipelineOptions{
			LineLimit:   textFileLineLimit,
			BufferLimit: TextFileBufferLimit,
		})
	if err != nil {
		return
	}

	notEmptyLineInfos := pipelines.Apply(pipelines.NewDiscardEmptyLinesTransform(task.Controller.Logger()), lineInfos)
	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), notEmptyLineInfos)
	parsed := pipelines.Apply(NewParseLineTransform(task.Controller.Logger()), lines)
	tskv := pipelines.Apply(NewTskvTransform(queueBatchSize, options.Cluster, options.TskvFormat), parsed)
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
	BaseLogPipelineOptions
}

func NewJSONLogPipeline(task timbertruck.TaskArgs, output pipelines.Output[pipelines.Row], options JSONLogPipelineOptions) (p *pipelines.Pipeline, err error) {
	textFileLineLimit := DefaultTextFileLineLimit
	if options.TextFileLineLimit > 0 {
		textFileLineLimit = options.TextFileLineLimit
	}
	queueBatchSize := DefaultQueueBatchSize
	if options.QueueBatchSize > 0 {
		queueBatchSize = options.QueueBatchSize
	}
	if queueBatchSize < textFileLineLimit {
		panic(fmt.Sprintf("bad options %v; queueBatchSize (%d) MUST BE >= textFileLineLimit (%d)", options, queueBatchSize, textFileLineLimit))
	}
	p, lineInfos, err := pipelines.NewTextPipeline(
		task.Controller.Logger().With("stagedpath", task.Path), task.Path, task.Position, pipelines.TextPipelineOptions{
			LineLimit:   textFileLineLimit,
			BufferLimit: TextFileBufferLimit,
		})
	if err != nil {
		return
	}

	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), lineInfos)
	lines = pipelines.Apply(NewValidateJSONTransform(task.Controller.Logger()), lines)
	batched := pipelines.Apply(NewBatchLinesTransform(queueBatchSize), lines)
	if options.AntisecretTransform != nil {
		batched = pipelines.Apply(options.AntisecretTransform, batched)
	}

	rows := pipelines.ApplyFunc(func(ctx context.Context, meta pipelines.RowMeta, value []byte, emit pipelines.EmitFunc[pipelines.Row]) {
		emit(ctx, meta, pipelines.Row{
			Payload: value,
			SeqNo:   meta.End.LogicalOffset,
		})
		task.Controller.NotifyProgress(meta.End)
	}, batched)

	pipelines.ApplyOutput(output, rows)
	return
}

type YSONLogPipelineOptions struct {
	BaseLogPipelineOptions
}

func NewYSONLogPipeline(task timbertruck.TaskArgs, output pipelines.Output[pipelines.Row], options YSONLogPipelineOptions) (p *pipelines.Pipeline, err error) {
	textFileLineLimit := DefaultTextFileLineLimit
	if options.TextFileLineLimit > 0 {
		textFileLineLimit = options.TextFileLineLimit
	}
	queueBatchSize := DefaultQueueBatchSize
	if options.QueueBatchSize > 0 {
		queueBatchSize = options.QueueBatchSize
	}
	if queueBatchSize < textFileLineLimit {
		panic(fmt.Sprintf("bad options %v; queueBatchSize (%d) MUST BE >= textFileLineLimit (%d)", options, queueBatchSize, textFileLineLimit))
	}
	p, lineInfos, err := pipelines.NewTextPipeline(
		task.Controller.Logger().With("stagedpath", task.Path), task.Path, task.Position, pipelines.TextPipelineOptions{
			LineLimit:   textFileLineLimit,
			BufferLimit: TextFileBufferLimit,
		})
	if err != nil {
		return
	}

	lines := pipelines.Apply(pipelines.NewDiscardTruncatedLinesTransform(task.Controller.Logger()), lineInfos)
	lines = pipelines.Apply(NewValidateYSONTransform(task.Controller.Logger()), lines)
	batched := pipelines.Apply(NewBatchLinesTransform(queueBatchSize), lines)
	if options.AntisecretTransform != nil {
		batched = pipelines.Apply(options.AntisecretTransform, batched)
	}

	rows := pipelines.ApplyFunc(func(ctx context.Context, meta pipelines.RowMeta, value []byte, emit pipelines.EmitFunc[pipelines.Row]) {
		emit(ctx, meta, pipelines.Row{
			Payload: value,
			SeqNo:   meta.End.LogicalOffset,
		})
		task.Controller.NotifyProgress(meta.End)
	}, batched)

	pipelines.ApplyOutput(output, rows)
	return
}
