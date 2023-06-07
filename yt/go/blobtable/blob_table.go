package blobtable

import (
	"context"
	"fmt"
	"io"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// BlobTableReader allows reading YT blob tables.
type BlobTableReader interface {
	// Next switches to the next blob in the stream.
	//
	// Reader must be positioned at the end of the previous blob.
	Next() bool

	// Err returns error associated with this reader.
	Err() error

	// ScanKey reads current row directly from underlying reader.
	ScanKey(key any) error

	// Read implements io.Reader, reading YT blob.
	Read([]byte) (int, error)

	// Close() frees resources associated with this reader.
	Close() error
}

type blobRow struct {
	PartIndex int    `yson:"part_index"`
	Data      []byte `yson:"data"`
}

type blobTableReader struct {
	tr yt.TableReader

	err error

	firstRow, lastRow bool
	readingBlob       bool
	nextPartIndex     int
	dataPos           int
	row               blobRow
}

func (r *blobTableReader) ScanKey(key any) error {
	if r.err != nil {
		return r.err
	}

	return r.tr.Scan(key)
}

func (r *blobTableReader) readRow() (ok, nextBlob bool) {
	if !r.tr.Next() {
		r.lastRow = true
		ok = false
		return
	}

	r.err = r.tr.Scan(&r.row)
	if r.err != nil {
		ok = false
		return
	}

	if r.row.PartIndex == 0 {
		r.nextPartIndex = 1
		ok = true
		nextBlob = true
	} else if r.row.PartIndex != r.nextPartIndex {
		ok = false
		r.err = fmt.Errorf("unexpected part index: %d != %d", r.row.PartIndex, r.nextPartIndex)
	} else {
		ok = true
		r.nextPartIndex++
	}

	return
}

func (r *blobTableReader) Next() bool {
	if r.readingBlob && r.err != nil {
		r.err = fmt.Errorf("call to Next() before reaching blob end")
	}

	if r.err != nil {
		return false
	}

	if r.lastRow {
		return false
	}

	r.readingBlob = true
	if r.firstRow {
		r.firstRow = false
		ok, _ := r.readRow()
		return ok
	} else {
		return true
	}
}

func (r *blobTableReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	if !r.readingBlob {
		return 0, io.EOF
	}

	for r.dataPos == len(r.row.Data) {
		r.dataPos = 0

		ok, next := r.readRow()
		if !ok {
			if r.err != nil {
				return 0, r.err
			}

			r.readingBlob = false
			return 0, io.EOF
		} else if next {
			r.readingBlob = false
			return 0, io.EOF
		}
	}

	n = copy(p, r.row.Data[r.dataPos:])
	r.dataPos += n
	return
}

func (r *blobTableReader) Close() error {
	return r.tr.Close()
}

func (r *blobTableReader) Err() error {
	if r.err != nil {
		return r.err
	}

	return r.tr.Err()
}

func ReadBlobTable(ctx context.Context, yc yt.TableClient, path ypath.YPath) (r BlobTableReader, err error) {
	tr, err := yc.ReadTable(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	return &blobTableReader{tr: tr, firstRow: true}, nil
}
