package pipelines

import (
	"bytes"
	"context"
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
	lineLimit int

	file *FollowingFile

	buffer  []byte
	begin   int
	end     int
	scanEnd int
}

func NewTextPipeline(filepath string, position FilePosition, options TextPipelineOptions) (p *Pipeline, s Stream[TextLine], err error) {
	if options.LineLimit <= 0 {
		panic(fmt.Sprintf("bad options %v; options.LineLimit MUST BE > 0", options))
	}

	if options.BufferLimit <= options.LineLimit {
		panic(fmt.Sprintf("bad options %v; options.LineLimit MUST BE > options.LineLimit", options))
	}

	file, err := OpenFollowingFile(filepath, position.LogicalOffset)
	if err != nil {
		return
	}

	root := &textFollower{
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

func (t *textFollower) emitLine(ctx context.Context, emit EmitFunc[TextLine]) {
	meta := RowMeta{}
	meta.Begin.LogicalOffset = t.file.FilePosition() - int64(t.end-t.begin)

	line := TextLine{}
	line.Bytes = t.buffer[t.begin:t.scanEnd]
	if len(line.Bytes) > t.lineLimit {
		line.Truncated = true
		line.Bytes = line.Bytes[:t.lineLimit]
	}

	meta.End.LogicalOffset = meta.Begin.LogicalOffset + int64(len(line.Bytes))
	emit(ctx, meta, line)
	t.begin = t.scanEnd
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
		if err != nil {
			break
		}
		t.end += read

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

	if err == io.EOF {
		err = nil
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
}

func (t *textFollower) Close(ctx context.Context, out EmitFunc[TextLine]) {
	_ = t.file.Close()
}

func NewDiscardTruncatedLinesTransform(logger *slog.Logger) Transform[TextLine, []byte] {
	return NewFuncTransform(func(ctx context.Context, meta RowMeta, line TextLine, emit EmitFunc[[]byte]) {
		if line.Truncated {
			logger.Warn("Detected truncated line", "offset", meta.Begin.LogicalOffset)
		} else {
			emit(ctx, meta, line.Bytes)
		}
	})
}

func NewDiscardEmptyLinesTransform(logger *slog.Logger) Transform[TextLine, TextLine] {
	return NewFuncTransform(func(ctx context.Context, meta RowMeta, line TextLine, emit EmitFunc[TextLine]) {
		if len(line.Bytes) == 0 {
			logger.Debug("Detected empty line, skipping", "offset", meta.Begin.LogicalOffset)
		} else {
			emit(ctx, meta, line)
		}
	})
}
