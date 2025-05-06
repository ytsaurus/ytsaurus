package pipelines

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

type LogFile interface {
	ReadContext(ctx context.Context, buf []byte) (read int, err error)
	FilePosition() FilePosition
	Stop()
	Close() error
}

func openLogFile(filepath string, offset int64) (f LogFile, err error) {
	if strings.HasSuffix(filepath, ".zst") {
		return newDecompressor(filepath, offset)
	} else {
		return OpenFollowingFile(filepath, offset)
	}
}

type decompressor struct {
	ff      *FollowingFile
	decoder *zstd.Decoder
	setCtx  func(context.Context)

	filePosition int64
}

func newDecompressor(filepath string, offset int64) (*decompressor, error) {
	ff, err := OpenFollowingFile(filepath, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open following file: %w", err)
	}
	fileReader := &ctxReader{ctx: context.Background(), read: ff.ReadContext}
	decoder, err := zstd.NewReader(fileReader,
		zstd.WithDecoderMaxMemory(512<<20), // 512 MiB
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd.Decoder: %w", err)
	}
	n, err := io.CopyN(io.Discard, decoder, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to skip %d bytes of uncompressed data: %w", offset, err)
	}
	if n != offset {
		return nil, fmt.Errorf("unexpected end of uncompressed log: expected to skip %d bytes, actually skipped %d", offset, n)
	}
	return &decompressor{
		ff:           ff,
		decoder:      decoder,
		setCtx:       func(ctx context.Context) { fileReader.ctx = ctx },
		filePosition: offset,
	}, nil
}

func (d *decompressor) ReadContext(ctx context.Context, buf []byte) (read int, err error) {
	d.setCtx(ctx)
	read, err = d.decoder.Read(buf)
	d.filePosition += int64(read)
	return
}

func (d *decompressor) FilePosition() FilePosition {
	return FilePosition{LogicalOffset: d.filePosition}
}

func (d *decompressor) Stop() {
	d.ff.Stop()
}

func (d *decompressor) Close() error {
	d.decoder.Close()
	return d.ff.Close()
}

type ctxReader struct {
	ctx  context.Context
	read func(ctx context.Context, buf []byte) (read int, err error)
}

func (r *ctxReader) Read(buf []byte) (read int, err error) {
	return r.read(r.ctx, buf)
}
