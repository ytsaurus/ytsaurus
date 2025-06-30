package httpclient

import (
	"bytes"
	"fmt"
	"io"

	"go.ytsaurus.tech/library/go/blockcodecs"
	"go.ytsaurus.tech/yt/go/yt"
)

type rowBatch struct {
	buf bytes.Buffer
}

func (b *rowBatch) Len() int {
	return b.buf.Len()
}

func (b *rowBatch) Write(p []byte) (n int, err error) {
	return b.buf.Write(p)
}

func (b *rowBatch) Close() error {
	return nil
}

type errTableWriter struct {
	err error
}

func (ew errTableWriter) Write(row any) error {
	return ew.err
}

func (ew errTableWriter) Commit() error {
	return ew.err
}

func (ew errTableWriter) Rollback() error {
	return ew.err
}

type rowBatchWriter struct {
	yt.TableWriter
	batch *rowBatch
}

func (bw *rowBatchWriter) Batch() yt.RowBatch {
	return bw.batch
}

func (c *httpClient) NewRowBatchWriter() yt.RowBatchWriter {
	batch := &rowBatch{}
	var w io.WriteCloser

	switch c.config.GetClientCompressionCodec() {
	case yt.ClientCodecGZIP, yt.ClientCodecNone:
		w = batch
	default:
		block, ok := c.config.GetClientCompressionCodec().BlockCodec()
		if !ok {
			err := fmt.Errorf("unsupported compression codec %d", c.config.GetClientCompressionCodec())
			return &rowBatchWriter{errTableWriter{err}, batch}
		}

		codec := blockcodecs.FindCodecByName(block)
		if codec == nil {
			err := fmt.Errorf("unsupported compression codec %q", block)
			return &rowBatchWriter{errTableWriter{err}, batch}
		}

		w = blockcodecs.NewEncoder(&batch.buf, codec)
	}

	tw, err := newTableWriter(w, nil, nil)
	if err != nil {
		return &rowBatchWriter{errTableWriter{err}, batch}
	}
	return &rowBatchWriter{tw, batch}
}
