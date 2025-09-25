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
}

type textFollower struct {
	logger    *slog.Logger
	lineLimit int

	file LogFile

	buffer  []byte
	begin   int
	end     int
	scanEnd int

	beginPosition FilePosition
}

func NewTextPipeline(
	logger *slog.Logger, filepath string, position FilePosition, options TextPipelineOptions,
) (p *Pipeline, s Stream[TextLine], err error) {
	if options.LineLimit <= 0 {
		panic(fmt.Sprintf("bad options %v; options.LineLimit MUST BE > 0", options))
	}

	if options.BufferLimit <= options.LineLimit {
		panic(fmt.Sprintf("bad options %v; options.LineLimit MUST BE > options.LineLimit", options))
	}

	file, err := openLogFile(logger, filepath, position)
	if err != nil {
		return
	}

	root := &textFollower{
		logger:    logger,
		lineLimit: options.LineLimit,
		file:      file,
		buffer:    make([]byte, options.BufferLimit),
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

func (t *textFollower) skipBrokenLine(ctx context.Context) (err error) {
	for !t.searchLineEnd() {
		t.begin = 0
		t.scanEnd = 0
		t.end, err = t.file.ReadContext(ctx, t.buffer)
		if err != nil {
			return
		}
	}
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
		panic("textFollower: InsideBlockOffset < remainingBytes for compressed file")
	}

	return FilePosition{
		LogicalOffset:       t.file.FilePosition().LogicalOffset - remainingBytes,
		BlockPhysicalOffset: t.file.FilePosition().BlockPhysicalOffset,
		InsideBlockOffset:   max(0, t.file.FilePosition().InsideBlockOffset-remainingBytes), // 0 for uncompressed file
	}
}

func (t *textFollower) emitLine(ctx context.Context, emit EmitFunc[TextLine]) {
	meta := RowMeta{}
	meta.Begin = t.beginPosition

	line := TextLine{}
	line.Bytes = t.buffer[t.begin:t.scanEnd]
	truncatedBytes := 0
	if len(line.Bytes) > t.lineLimit {
		line.Truncated = true
		truncatedBytes = len(line.Bytes) - t.lineLimit
		line.Bytes = line.Bytes[:t.lineLimit]
	}

	lineEndPos := t.lineEndPosition()
	meta.End = FilePosition{
		LogicalOffset:       lineEndPos.LogicalOffset - int64(truncatedBytes),
		BlockPhysicalOffset: lineEndPos.BlockPhysicalOffset,
		InsideBlockOffset:   max(0, lineEndPos.InsideBlockOffset-int64(truncatedBytes)), // 0 for uncompressed file
	}
	emit(ctx, meta, line)

	t.beginPosition = lineEndPos
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

func (t *textFollower) Process(ctx context.Context, _ RowMeta, in Impulse, emit EmitFunc[TextLine]) {
	var err error
	for {
		avail := len(t.buffer) - t.end
		if avail < t.lineLimit {
			t.rewind()
		}
		if len(t.buffer) <= t.end {
			panic(fmt.Sprintf("Internal error len(t.buffer) == %v && t.end == %v", len(t.buffer), t.end))
		}
		var read int
		read, err = t.file.ReadContext(ctx, t.buffer[t.end:])
		t.end += read
		if err != nil {
			break
		}

		for t.searchLineEnd() {
			t.emitLine(ctx, emit)
		}

		if t.scanEnd != t.end {
			panic(fmt.Sprintf("internal error end != scanEnd: %v != %v", t.end, t.scanEnd))
		}

		if t.end-t.begin > t.lineLimit {
			t.emitLine(ctx, emit)
			err = t.skipBrokenLine(ctx)
			if err != nil {
				break
			}
		}
	}

	if errors.Is(err, io.EOF) {
		t.emitRemainingLines(ctx, emit)
	} else if errors.Is(err, io.ErrUnexpectedEOF) {
		t.logger.Warn("Unexpected EOF: file may be corrupted", "error", err, "end_position", t.file.FilePosition())
		t.emitRemainingLines(ctx, emit)
	} else if err != nil && !errors.Is(err, context.Canceled) {
		panic(fmt.Sprintf("unexpected error while reading file: %s", err.Error()))
	}
}

func (t *textFollower) Close(ctx context.Context, out EmitFunc[TextLine]) {
	_ = t.file.Close()
}

func NewDiscardTruncatedLinesTransform(logger *slog.Logger) Transform[TextLine, []byte] {
	return NewFuncTransform(func(ctx context.Context, meta RowMeta, line TextLine, emit EmitFunc[[]byte]) {
		if line.Truncated {
			logger.Warn("Detected truncated line", "offset", meta.Begin)
		} else {
			emit(ctx, meta, line.Bytes)
		}
	})
}

func NewDiscardEmptyLinesTransform(logger *slog.Logger) Transform[TextLine, TextLine] {
	return NewFuncTransform(func(ctx context.Context, meta RowMeta, line TextLine, emit EmitFunc[TextLine]) {
		if len(line.Bytes) == 0 {
			logger.Debug("Detected empty line, skipping", "offset", meta.Begin)
		} else {
			emit(ctx, meta, line)
		}
	})
}
