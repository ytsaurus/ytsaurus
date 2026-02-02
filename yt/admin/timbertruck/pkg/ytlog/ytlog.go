package ytlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
	"go.ytsaurus.tech/yt/go/yson"
)

//
// ParsedLineCursor
//

type ParsedLine struct {
	timestamp   []byte
	logLevel    []byte
	logCategory []byte
	logMessage  []byte
}

var errBrokenLine = errors.New("log line is broken")

func cutByte(s []byte, sep byte) ([]byte, []byte, bool) {
	if i := bytes.IndexByte(s, sep); i >= 0 {
		return s[:i], s[i+1:], true
	} else {
		return s, nil, false
	}
}

func ParseLine(line []byte) (result ParsedLine, err error) {
	result.logMessage = line

	var split bool
	result.timestamp, result.logMessage, _ = cutByte(result.logMessage, '\t')
	result.logLevel, result.logMessage, _ = cutByte(result.logMessage, '\t')
	result.logCategory, result.logMessage, split = cutByte(result.logMessage, '\t')

	for pos, value := range result.logMessage {
		if value == '\t' {
			result.logMessage[pos] = ' '
		}
	}

	if !split {
		err = errBrokenLine
	}
	return
}

func NewParseLineTransform(logger *slog.Logger, onSkippedRow func(data io.WriterTo, info pipelines.SkippedRowInfo)) pipelines.Transform[[]byte, ParsedLine] {
	return pipelines.NewFuncTransform(func(ctx context.Context, meta pipelines.RowMeta, line []byte, emit pipelines.EmitFunc[ParsedLine]) {
		result, err := ParseLine(line)
		if err != nil {
			onSkippedRow(bytes.NewReader(line), pipelines.SkippedRowInfo{
				Reason: pipelines.SkipRowReasonUnparsed,
				Offset: meta.Begin,
				Attrs:  map[string]any{"format": "text", "row_size": len(line), "error": err.Error()},
			})
			return
		}
		emit(ctx, meta, result)
	})
}

// TskvLineTransform converts ParsedLine to tskv-formatted bytes.
func NewTskvLineTransform(cluster string, tskvFormat string) pipelines.Transform[ParsedLine, []byte] {
	return &tskvLineTransform{
		cluster:    cluster,
		tskvFormat: tskvFormat,
	}
}

type tskvLineTransform struct {
	cluster    string
	tskvFormat string
}

func (t *tskvLineTransform) Process(ctx context.Context, meta pipelines.RowMeta, parsedLine ParsedLine, emit pipelines.EmitFunc[[]byte]) {
	var buf bytes.Buffer

	buf.WriteString("tskv\ttskv_format=")
	buf.WriteString(t.tskvFormat)
	buf.WriteString("\tcluster=")
	buf.WriteString(t.cluster)

	buf.WriteString("\ttimestamp=")
	buf.Write(parsedLine.timestamp)

	buf.WriteString("\tlog_level=")
	buf.Write(parsedLine.logLevel)

	buf.WriteString("\tlog_category=")
	buf.Write(parsedLine.logCategory)

	buf.WriteString("\tlog_message=")
	buf.Write(parsedLine.logMessage)

	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}

	emit(ctx, meta, buf.Bytes())
}

func (t *tskvLineTransform) Close(ctx context.Context, emit pipelines.EmitFunc[[]byte]) {
}

// Create new pipelines.TextLine -> pipelines.TextLine transform.
//
// This transform filters out lines that are not valid json.
func NewValidateJSONTransform(logger *slog.Logger, onSkippedRow func(data io.WriterTo, info pipelines.SkippedRowInfo)) pipelines.Transform[[]byte, []byte] {
	return pipelines.NewFuncTransform(func(ctx context.Context, meta pipelines.RowMeta, in []byte, emit pipelines.EmitFunc[[]byte]) {
		if !json.Valid(in) {
			onSkippedRow(bytes.NewReader(in), pipelines.SkippedRowInfo{
				Reason: pipelines.SkipRowReasonUnparsed,
				Offset: meta.Begin,
				Attrs:  map[string]any{"format": "json", "row_size": len(in)},
			})
			return
		}
		emit(ctx, meta, in)
	})
}

// This transform filters out lines that are not valid yson.
func NewValidateYSONTransform(logger *slog.Logger, onSkippedRow func(data io.WriterTo, info pipelines.SkippedRowInfo)) pipelines.Transform[[]byte, []byte] {
	return pipelines.NewFuncTransform(func(ctx context.Context, meta pipelines.RowMeta, in []byte, emit pipelines.EmitFunc[[]byte]) {
		if err := yson.ValidListFragment(in); err != nil {
			onSkippedRow(bytes.NewReader(in), pipelines.SkippedRowInfo{
				Reason: pipelines.SkipRowReasonUnparsed,
				Offset: meta.Begin,
				Attrs:  map[string]any{"format": "yson", "row_size": len(in), "error": err.Error()},
			})
			return
		}
		emit(ctx, meta, in)
	})
}

func NewBatchLinesTransform(bufferLimit int, flushTimeout time.Duration) pipelines.Transform[[]byte, []byte] {
	t := &batchLinesTransform{
		bufferLimit:  bufferLimit,
		flushTimeout: flushTimeout,
		stopTimerCh:  make(chan struct{}),
		doneTimerCh:  make(chan struct{}),
	}
	if flushTimeout > 0 {
		t.timer = time.NewTimer(flushTimeout)
		t.timer.Stop()
	}
	return t
}

type batchLinesTransform struct {
	mu          sync.Mutex
	buffer      bytes.Buffer
	bufferLimit int

	flushTimeout time.Duration
	timer        *time.Timer
	timerStarted bool
	stopTimerCh  chan struct{}
	doneTimerCh  chan struct{}

	meta pipelines.RowMeta
	ctx  context.Context
}

func (b *batchLinesTransform) Process(ctx context.Context, meta pipelines.RowMeta, line []byte, emit pipelines.EmitFunc[[]byte]) {
	if b.timer != nil && !b.timerStarted {
		b.timerStarted = true
		go b.timerLoop(emit)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.buffer.Len()+len(line) >= b.bufferLimit {
		b.flushLocked(emit)
	}

	if b.buffer.Len() == 0 {
		b.meta = meta
		if b.timer != nil {
			b.timer.Reset(b.flushTimeout)
		}
	}

	b.buffer.Write(line)
	if b.buffer.Bytes()[b.buffer.Len()-1] != '\n' {
		b.buffer.WriteByte('\n')
	}
	b.ctx = ctx
	b.meta.End = meta.End
}

func (b *batchLinesTransform) timerLoop(emit pipelines.EmitFunc[[]byte]) {
	defer close(b.doneTimerCh)

	for {
		select {
		case <-b.stopTimerCh:
			return
		case <-b.timer.C:
			b.flush(emit)
		}
	}
}

func (b *batchLinesTransform) flush(emit pipelines.EmitFunc[[]byte]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.flushLocked(emit)
}

func (b *batchLinesTransform) flushLocked(emit pipelines.EmitFunc[[]byte]) {
	if b.buffer.Len() == 0 {
		return
	}

	if b.timer != nil {
		b.timer.Stop()
	}

	emit(b.ctx, b.meta, b.buffer.Bytes())
	b.buffer.Reset()
	b.meta = pipelines.RowMeta{}
}

func (b *batchLinesTransform) Close(ctx context.Context, emit pipelines.EmitFunc[[]byte]) {
	close(b.stopTimerCh)
	if b.timerStarted {
		<-b.doneTimerCh
	}
	b.flush(emit)
}
