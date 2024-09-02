// This library provides a way to build pipelines processing log files.
//
// Key concepts of this library.
//
// Stream -- logical stream of records of a certain golang type.
// User can apply transforms to the stream to create new streams.
// This stream is not a real stream, it is just a description of the pipeline.
//
// Pipeline -- structure that describes the whole processing of a single file.
// Together with creating a pipeline, the root stream is returned.
// After that, the user can add transforms and get new streams,
// and also direct streams to outputs.
//
// Transform -- object that transforms a stream (for example, parses a text log line).
// Applying transforms to streams can result in new streams.
//
// Output -- object that writes a stream somewhere, for example, to YT.

package pipelines

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Logical stream of objects.
//
// Transform can be applied to a stream to create new stream.
//
// Output can be applied to a stream to write records of this stream somewhere.
//
// Each stream must be connected to at least one transform or output.
// Otherwise, `Pipeline.run` will panic.
type Stream[T any] struct {
	apply func(output Output[T])
}

type EmitFunc[T any] func(context.Context, RowMeta, T)

// Interface for stream transform.
// Transform reads elements from input stream and writes new elements to output stream.
// There is no possibility to return error and abort processing. Transforms must try to process input data no matter what.
type Transform[In any, Out any] interface {
	// Process single element of input stream.
	//
	// - `ctx` golang context for pipeline. If context is canceled transform should stop the work ASAP (and return ctx.Err()).
	//   Process should no release resources inside Process(), since Close() will be called anyway.
	//
	// - `meta` is a metainformatsion about current element, contains info about position in the source file.
	//   Transform can modify meta, e.g. a transform that merges rows can output a range in the file for the merged result.
	// - `in` input element from source stream
	// - `out` output stream
	Process(ctx context.Context, meta RowMeta, in In, emit EmitFunc[Out])

	// Close finishes the work, flushes the results and releases all aquired resources.
	Close(ctx context.Context, out EmitFunc[Out])
}

// Apply transform to stream to get new stream.
func Apply[In any, Out any](t Transform[In, Out], stream Stream[In]) Stream[Out] {
	bound := &boundTransform[In, Out]{
		transform: t,
		output:    BrokenOutput[Out]{},
	}

	stream.apply(bound)

	return Stream[Out]{
		apply: bound.addOutput,
	}
}

// Shortcut to apply map function to get new stream.
func ApplyMapFunc[In any, Out any](f func(in In) Out, s Stream[In]) Stream[Out] {
	transform := mapFuncTransform[In, Out]{
		f: f,
	}
	return Apply(transform, s)
}

func ApplyFunc[In, Out any](f func(context.Context, RowMeta, In, EmitFunc[Out]), s Stream[In]) Stream[Out] {
	transform := funcTransform[In, Out]{
		f: f,
	}
	return Apply(transform, s)
}

func ApplyOutput[In any](o Output[In], s Stream[In]) {
	s.apply(o)
}

func ApplyOutputFunc[In any](f func(context.Context, RowMeta, In), s Stream[In]) {
	output := funcOutput[In]{
		f: f,
	}
	s.apply(output)
}

type mapFuncTransform[In any, Out any] struct {
	f func(In) Out
}

func (t mapFuncTransform[In, Out]) Process(ctx context.Context, meta RowMeta, in In, emit EmitFunc[Out]) {
	emit(ctx, meta, t.f(in))
}

func (t mapFuncTransform[In, Out]) Close(ctx context.Context, _ EmitFunc[Out]) {
}

type funcTransform[In any, Out any] struct {
	f func(context.Context, RowMeta, In, EmitFunc[Out])
}

func (t funcTransform[In, Out]) Process(ctx context.Context, meta RowMeta, in In, out EmitFunc[Out]) {
	t.f(ctx, meta, in, out)
}

func (t funcTransform[In, Out]) Close(_ context.Context, _ EmitFunc[Out]) {
}

func NewFuncTransform[In any, Out any](f func(context.Context, RowMeta, In, EmitFunc[Out])) Transform[In, Out] {
	return funcTransform[In, Out]{
		f: f,
	}
}

//
// OUTPUT
//

type Output[T any] interface {
	Add(ctx context.Context, meta RowMeta, row T)
	Close(ctx context.Context)
}

type funcOutput[T any] struct {
	f func(context.Context, RowMeta, T)
}

func (o funcOutput[T]) Add(ctx context.Context, meta RowMeta, in T) {
	o.f(ctx, meta, in)
}

func (o funcOutput[T]) Close(ctx context.Context) {
}

// Implementation

type BrokenOutput[T any] struct {
}

func (o BrokenOutput[T]) Add(ctx context.Context, meta RowMeta, row T) {
	panic("Unconnected writer")
}

func (o BrokenOutput[T]) Close(ctx context.Context) {
	panic("Unconnected writer")
}

type multiOutput[T any] struct {
	outputs []Output[T]
}

func (o multiOutput[T]) Add(ctx context.Context, meta RowMeta, row T) {
	for i := range o.outputs {
		o.outputs[i].Add(ctx, meta, row)
	}
}

func (o multiOutput[T]) Close(ctx context.Context) {
	for i := range o.outputs {
		o.outputs[i].Close(ctx)
	}
}

func NewMultiOutput[T any](outputs ...Output[T]) Output[T] {
	return multiOutput[T]{
		outputs: outputs,
	}
}

type boundTransform[In any, Out any] struct {
	transform Transform[In, Out]
	output    Output[Out]
	emit      EmitFunc[Out]
}

func (t *boundTransform[In, Out]) Add(ctx context.Context, meta RowMeta, row In) {
	t.transform.Process(ctx, meta, row, t.emit)
}

func (t *boundTransform[In, Out]) Close(ctx context.Context) {
	t.transform.Close(ctx, t.emit)
	t.output.Close(ctx)
}

func (t *boundTransform[In, Out]) addOutput(o Output[Out]) {
	switch t.output.(type) {
	case nil, BrokenOutput[Out]:
		t.output = o
		t.emit = o.Add
	case multiOutput[Out]:
		mo, ok := t.output.(multiOutput[Out])
		if !ok {
			panic("")
		}
		mo.outputs = append(mo.outputs, o)
		t.emit = mo.Add
	default:
		t.output = &multiOutput[Out]{
			outputs: []Output[Out]{t.output, o},
		}
		t.emit = t.output.Add
	}
}

// FilePosition describes a position in the possibly compressed file.
type FilePosition struct {
	// Logical offset in the file. I.e. how many (uncompressed) bytes should be read
	// to get to the current position.
	// If file is uncompressed it's ordinary offset that can be "seeked".
	LogicalOffset int64 `json:"logical_offset"`

	// Physical offset for the current compression block.
	// It's always 0 for uncompressed file.
	BlockPhysicalOffset int64 `json:"block_physical_offset"`

	// Logical offset inside compression block. I.e. how many (uncompressed) data inside block should be read
	// to get to the current position.
	// For uncompressed file it's always 0.
	InsideBlockOffset int64 `json:"inside_block_offset"`
}

// Create FilePosition for uncompressed file.
func UncompressedFilePosition(offset int64) FilePosition {
	return FilePosition{
		LogicalOffset: offset,
	}
}

// Metainformation for stream row.
// Describes span in the source file for current row.
type RowMeta struct {
	Begin FilePosition
	End   FilePosition
}

// Log file pipeline.
type Pipeline struct {
	root Output[Impulse]

	notifyCompleteF func()
	isComplete      atomic.Bool
}

// Run the pipeline.
func (p *Pipeline) Run(ctx context.Context) error {
	p.root.Add(ctx, RowMeta{}, Impulse{})
	p.root.Close(ctx)
	if ctx.Err() != nil {
		return fmt.Errorf("run aborted: %w", ctx.Err())
	}
	return nil
}

// Notify the pipeline, that source file is complete and there is no need to wait for additional data.
// Pipeline completes only when all file data is processed.
func (p *Pipeline) NotifyComplete() {
	if p.isComplete.CompareAndSwap(false, true) {
		if p.notifyCompleteF != nil {
			p.notifyCompleteF()
		}
	}
}

func NewPipelineFromRootTransform[T any](root Transform[Impulse, T], notifyCompleteF func()) (p *Pipeline, s Stream[T]) {
	bound := &boundTransform[Impulse, T]{
		transform: root,
		output:    BrokenOutput[T]{},
	}

	p = &Pipeline{
		root:            bound,
		notifyCompleteF: notifyCompleteF,
	}
	s.apply = bound.addOutput
	return
}

// ImpulseValue это уникальное значение, которое используется для того, чтобы запустить pipeline.
// Нужно только разработчикам новых типов pipeline'ов.
type Impulse struct{}
