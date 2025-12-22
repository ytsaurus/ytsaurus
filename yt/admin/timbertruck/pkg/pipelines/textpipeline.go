package pipelines

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// Representation of a text file line.
type TextLine struct {
	// Line including '\n' (last line might miss '\n')
	Bytes []byte

	// Whether line was truncated.
	Truncated bool
}

type TextPipelineOptions struct {
	// Maximum size of a line. Lines bigger will be truncated. LineLimit must be > 0.
	LineLimit int

	// Size of internal buffer. BufferLimit must be > LineLimit.
	BufferLimit int

	// OnTruncatedRow is called when a row is truncated.
	OnTruncatedRow func(data io.WriterTo, info SkippedRowInfo)
}

type textFollower struct {
	logger         *slog.Logger
	lineLimit      int
	onTruncatedRow func(data io.WriterTo, info SkippedRowInfo)

	file LogFile

	buffer  []byte
	begin   int
	end     int
	scanEnd int

	beginPosition FilePosition
	readErr       error
}

func NewTextPipeline(
	logger *slog.Logger, filepath string, position FilePosition, options TextPipelineOptions,
) (p *Pipeline, s Stream[TextLine], err error) {
	if options.LineLimit <= 0 {
		panic(fmt.Sprintf("bad options %v; options.LineLimit MUST BE > 0", options))
	}

	if options.BufferLimit <= options.LineLimit {
		panic(fmt.Sprintf("bad options %v; options.BufferLimit MUST BE > options.LineLimit", options))
	}

	file, err := openLogFile(logger, filepath, position)
	if err != nil {
		return
	}

	root := &textFollower{
		logger:         logger,
		lineLimit:      options.LineLimit,
		onTruncatedRow: options.OnTruncatedRow,
		file:           file,
		buffer:         make([]byte, options.BufferLimit),
	}
	p, s = NewPipelineFromRootTransform(root, root.stop)
	return
}

func (t *textFollower) stop() {
	t.file.Stop()
}

func (t *textFollower) rewind() {
	copy(t.buffer, t.buffer[t.begin:t.end])
	t.end = t.end - t.begin
	t.scanEnd = t.scanEnd - t.begin
	t.begin = 0
}

// writeBrokenLineTo writes a line that hasn't been fully read yet (no newline found in buffer).
func (t *textFollower) writeBrokenLineTo(ctx context.Context, writer io.Writer) (n int64, err error) {
	written, err := writer.Write(t.buffer[t.begin:t.end])
	n = int64(written)
	if err != nil {
		return
	}

	for {
		t.begin = 0
		t.scanEnd = 0
		t.end, err = t.file.ReadContext(ctx, t.buffer)
		if err != nil {
			t.readErr = err
			return
		}

		if t.searchLineEnd() {
			break
		}

		written, err = writer.Write(t.buffer[:t.end])
		n += int64(written)
		if err != nil {
			return
		}
	}
	written, err = writer.Write(t.buffer[:t.scanEnd])
	n += int64(written)

	t.beginPosition = t.lineEndPosition()
	t.begin = t.scanEnd
	return
}

func (t *textFollower) searchLineEnd() (found bool) {
	position := bytes.IndexByte(t.buffer[t.scanEnd:t.end], '\n')
	if position == -1 {
		t.scanEnd = t.end
		return false
	}
	t.scanEnd = t.scanEnd + position + 1
	return true
}

func (t *textFollower) lineEndPosition() FilePosition {
	// Invariant: t.end and t.scanEnd are always within the same block,
	// because t.file.ReadContext reads at most one block at a time.
	remainingBytes := int64(t.end - t.scanEnd)
	if (t.file.FilePosition().BlockPhysicalOffset > 0 || t.file.FilePosition().InsideBlockOffset > 0) &&
		t.file.FilePosition().InsideBlockOffset < remainingBytes {
		panic(fmt.Sprintf("textFollower: InsideBlockOffset (%d) < remainingBytes (%d) for compressed file", t.file.FilePosition().InsideBlockOffset, remainingBytes))
	}

	return FilePosition{
		LogicalOffset:       t.file.FilePosition().LogicalOffset - remainingBytes,
		BlockPhysicalOffset: t.file.FilePosition().BlockPhysicalOffset,
		InsideBlockOffset:   max(0, t.file.FilePosition().InsideBlockOffset-remainingBytes), // 0 for uncompressed file
	}
}

// computeMetaEnd computes the end position for a line, accounting for truncated bytes.
func (t *textFollower) computeMetaEnd(truncatedBytes int64) FilePosition {
	lineEndPos := t.lineEndPosition()
	return FilePosition{
		LogicalOffset:       lineEndPos.LogicalOffset - truncatedBytes,
		BlockPhysicalOffset: lineEndPos.BlockPhysicalOffset,
		InsideBlockOffset:   max(0, lineEndPos.InsideBlockOffset-truncatedBytes),
	}
}

func (t *textFollower) emitAllLines(ctx context.Context, emit EmitFunc[TextLine]) {
	for t.searchLineEnd() {
		t.emitLine(ctx, emit)
	}
	if t.scanEnd != t.end {
		panic(fmt.Sprintf("internal error end != scanEnd: %v != %v", t.end, t.scanEnd))
	}
}

func (t *textFollower) emitLine(ctx context.Context, emit EmitFunc[TextLine]) {
	line := TextLine{}
	line.Bytes = t.buffer[t.begin:t.scanEnd]
	truncatedBytes := 0
	if len(line.Bytes) > t.lineLimit {
		t.onTruncatedRow(
			bytes.NewBuffer(line.Bytes),
			SkippedRowInfo{
				Reason: SkipRowReasonTruncated,
				Offset: t.beginPosition,
				Attrs:  map[string]any{"size": len(line.Bytes)},
			},
		)
		line.Truncated = true
		truncatedBytes = len(line.Bytes) - t.lineLimit
		line.Bytes = line.Bytes[:t.lineLimit]
	}

	meta := RowMeta{
		Begin: t.beginPosition,
		End:   t.computeMetaEnd(int64(truncatedBytes)),
	}
	emit(ctx, meta, line)

	t.beginPosition = t.lineEndPosition()
	t.begin = t.scanEnd
}

func (t *textFollower) emitRemainingLines(ctx context.Context, emit EmitFunc[TextLine]) {
	for t.searchLineEnd() {
		t.emitLine(ctx, emit)
	}
	if t.begin < t.end {
		if t.scanEnd != t.end {
			panic(fmt.Sprintf("Internal error: %v != %v", t.scanEnd, t.end))
		}
		t.emitLine(ctx, emit)
	}
}

// emitOversizedLine handles lines that exceed lineLimit and have no newline in the buffer.
// It emits the first lineLimit bytes and writes the full line to skipped rows file.
func (t *textFollower) emitOversizedLine(ctx context.Context, emit EmitFunc[TextLine]) {
	if t.begin+t.lineLimit > t.end {
		panic(fmt.Sprintf("emitOversizedLine: buffer doesn't have enough data for lineLimit: begin=%d, lineLimit=%d, end=%d", t.begin, t.lineLimit, t.end))
	}
	line := TextLine{
		Bytes:     t.buffer[t.begin : t.begin+t.lineLimit],
		Truncated: true,
	}
	meta := RowMeta{
		Begin: t.beginPosition,
		End:   t.computeMetaEnd(int64(t.end - t.begin - t.lineLimit)),
	}
	emit(ctx, meta, line)

	// The line is not fully read yet (no newline found), so we use brokenLineWriter
	// to continue reading and streaming the full line from the file.
	brokenLineWriterCalled := false
	brokenLineWriter := writerToFunc(func(w io.Writer) (int64, error) {
		if brokenLineWriterCalled {
			panic("writeBrokenLineTo was called more than once")
		}
		brokenLineWriterCalled = true
		return t.writeBrokenLineTo(ctx, w)
	})
	t.onTruncatedRow(brokenLineWriter, SkippedRowInfo{
		Reason: SkipRowReasonTruncated,
		Offset: t.beginPosition,
	})

	if !brokenLineWriterCalled {
		panic("onTruncatedRow did not call WriteTo on brokenLineWriter")
	}
}

func (t *textFollower) Process(ctx context.Context, _ RowMeta, in Impulse, emit EmitFunc[TextLine]) {
	for {
		avail := len(t.buffer) - t.end
		if avail < t.lineLimit {
			t.rewind()
		}
		if len(t.buffer) <= t.end {
			panic(fmt.Sprintf("Internal error len(t.buffer) == %v && t.end == %v", len(t.buffer), t.end))
		}
		var read int
		read, t.readErr = t.file.ReadContext(ctx, t.buffer[t.end:])
		t.end += read
		if t.readErr != nil {
			break
		}

		t.emitAllLines(ctx, emit)

		if t.end-t.begin > t.lineLimit {
			t.emitOversizedLine(ctx, emit)

			if t.readErr != nil {
				break
			}
			// After processing oversized line, emit all remaining lines in buffer.
			t.emitAllLines(ctx, emit)
		}
	}

	if errors.Is(t.readErr, io.EOF) {
		t.emitRemainingLines(ctx, emit)
	} else if errors.Is(t.readErr, io.ErrUnexpectedEOF) {
		t.logger.Warn("Unexpected EOF: file may be corrupted", "error", t.readErr, "end_position", t.file.FilePosition())
		t.emitRemainingLines(ctx, emit)
	} else if t.readErr != nil && !errors.Is(t.readErr, context.Canceled) {
		panic(fmt.Sprintf("unexpected error while reading file: %s", t.readErr.Error()))
	}
}

func (t *textFollower) Close(ctx context.Context, out EmitFunc[TextLine]) {
	_ = t.file.Close()
}

// writerToFunc adapts a function to io.WriterTo interface.
type writerToFunc func(io.Writer) (int64, error)

func (f writerToFunc) WriteTo(w io.Writer) (int64, error) {
	return f(w)
}

func NewDiscardTruncatedLinesTransform(logger *slog.Logger) Transform[TextLine, []byte] {
	return NewFuncTransform(func(ctx context.Context, meta RowMeta, line TextLine, emit EmitFunc[[]byte]) {
		if line.Truncated {
			logger.Debug("Detected truncated line", "offset", meta.Begin)
		} else {
			emit(ctx, meta, line.Bytes)
		}
	})
}

func NewDiscardEmptyLinesTransform(logger *slog.Logger) Transform[TextLine, TextLine] {
	return NewFuncTransform(func(ctx context.Context, meta RowMeta, line TextLine, emit EmitFunc[TextLine]) {
		if len(bytes.TrimSpace(line.Bytes)) == 0 {
			logger.Debug("Detected empty line, skipping", "offset", meta.Begin)
		} else {
			emit(ctx, meta, line)
		}
	})
}
