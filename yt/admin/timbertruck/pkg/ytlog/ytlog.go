package ytlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"

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

func NewParseLineTransform(logger *slog.Logger) pipelines.Transform[[]byte, ParsedLine] {
	return pipelines.NewFuncTransform(func(ctx context.Context, meta pipelines.RowMeta, line []byte, emit pipelines.EmitFunc[ParsedLine]) {
		result, err := ParseLine(line)
		if err != nil {
			logger.Warn("Detected broken line", "offset", meta.Begin.LogicalOffset)
		} else {
			emit(ctx, meta, result)
		}
	})
}

//
// TskvCursor
//

type tskvTransform struct {
	buffer      bytes.Buffer
	bufferLimit int
	cluster     string
	tskvFormat  string

	meta pipelines.RowMeta
}

func (t *tskvTransform) Process(ctx context.Context, meta pipelines.RowMeta, parsedLine ParsedLine, emit pipelines.EmitFunc[[]byte]) {
	if t.buffer.Len() == 0 {
		t.meta.Begin = meta.Begin
	}
	lenBefore := t.buffer.Len()

	t.buffer.WriteString("tskv\ttskv_format=")
	t.buffer.WriteString(t.tskvFormat)
	t.buffer.WriteString("\tcluster=")
	t.buffer.WriteString(t.cluster)

	t.buffer.WriteString("\ttimestamp=")
	t.buffer.Write(parsedLine.timestamp)

	t.buffer.WriteString("\tlog_level=")
	t.buffer.Write(parsedLine.logLevel)

	t.buffer.WriteString("\tlog_category=")
	t.buffer.Write(parsedLine.logCategory)

	t.buffer.WriteString("\tlog_message=")
	t.buffer.Write(parsedLine.logMessage)

	if t.buffer.Bytes()[t.buffer.Len()-1] != '\n' {
		t.buffer.WriteByte('\n')
	}

	if t.buffer.Len() > t.bufferLimit {
		t.flush(ctx, lenBefore, emit)
		t.meta.Begin = meta.Begin
	}
	t.meta.End = meta.End
}

func (t *tskvTransform) flush(ctx context.Context, size int, emit pipelines.EmitFunc[[]byte]) {
	record := t.buffer.Bytes()[:size]
	emit(ctx, t.meta, record)
	remainData := t.buffer.Bytes()[size:]
	copy(t.buffer.Bytes(), remainData)
	t.buffer.Truncate(len(remainData))
}

func (t *tskvTransform) Close(ctx context.Context, emit pipelines.EmitFunc[[]byte]) {
	len := t.buffer.Len()
	if len > 0 {
		t.flush(ctx, len, emit)
	}
}

func NewTskvTransform(bufferLimit int, cluster string, tskvFormat string) pipelines.Transform[ParsedLine, []byte] {
	return &tskvTransform{
		bufferLimit: bufferLimit,
		cluster:     cluster,
		tskvFormat:  tskvFormat,
	}
}

// Create new pipelines.TextLine -> pipelines.TextLine transform.
//
// This transform filters out lines that are not valid json.
func NewValidateJSONTransform(logger *slog.Logger) pipelines.Transform[[]byte, []byte] {
	return pipelines.NewFuncTransform(func(ctx context.Context, meta pipelines.RowMeta, in []byte, emit pipelines.EmitFunc[[]byte]) {
		if !json.Valid(in) {
			logger.Warn("found invalid json", "offset", meta.Begin.LogicalOffset)
			return
		}
		emit(ctx, meta, in)
	})
}

// This transform filters out lines that are not valid yson.
func NewValidateYSONTransform(logger *slog.Logger) pipelines.Transform[[]byte, []byte] {
	return pipelines.NewFuncTransform(func(ctx context.Context, meta pipelines.RowMeta, in []byte, emit pipelines.EmitFunc[[]byte]) {
		if err := yson.ValidListFragment(in); err != nil {
			logger.Warn("found invalid yson", "offset", meta.Begin.LogicalOffset)
			return
		}
		emit(ctx, meta, in)
	})
}

func NewBatchLinesTransform(bufferLimit int) pipelines.Transform[[]byte, []byte] {
	return &batchLinesTransform{
		bufferLimit: bufferLimit,
	}
}

type batchLinesTransform struct {
	buffer      bytes.Buffer
	bufferLimit int

	meta pipelines.RowMeta
}

func (b *batchLinesTransform) Process(ctx context.Context, meta pipelines.RowMeta, line []byte, emit pipelines.EmitFunc[[]byte]) {
	if b.buffer.Len() == 0 {
		b.meta.Begin = meta.Begin
	}
	lenBefore := b.buffer.Len()

	b.buffer.Write(line)

	if b.buffer.Bytes()[b.buffer.Len()-1] != '\n' {
		b.buffer.WriteByte('\n')
	}

	if b.buffer.Len() > b.bufferLimit {
		b.flush(ctx, lenBefore, emit)
		b.meta.Begin = meta.Begin
	}
	b.meta.End = meta.End
}

func (b *batchLinesTransform) flush(ctx context.Context, size int, emit pipelines.EmitFunc[[]byte]) {
	record := b.buffer.Bytes()[:size]
	emit(ctx, b.meta, record)
	remainData := b.buffer.Bytes()[size:]
	copy(b.buffer.Bytes(), remainData)
	b.buffer.Truncate(len(remainData))
}

func (b *batchLinesTransform) Close(ctx context.Context, emit pipelines.EmitFunc[[]byte]) {
	len := b.buffer.Len()
	if len > 0 {
		b.flush(ctx, len, emit)
	}
}
