package smartreader

import (
	"context"
	"fmt"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func NewReader(
	ctx context.Context,
	tx yt.Tx, txOwned bool,
	l log.Structured,
	path ypath.YPath,
	options *yt.ReadTableOptions,
) (yt.TableReader, error) {
	lock, err := tx.LockNode(ctx, path, yt.LockSnapshot, nil)
	if err != nil {
		return nil, err
	}

	var rich *ypath.Rich
	switch p := path.(type) {
	case ypath.Path:
		rich, err = ypath.Parse(p.String())
		if err != nil {
			return nil, err
		}
	case *ypath.Rich:
		rich = p
	default:
		return nil, fmt.Errorf("unsupported path type: %T", path)
	}

	ranges := rich.Ranges
	if len(ranges) == 0 {
		ranges = []ypath.Range{{}}
	}

	return &smartReader{
		ctx:        ctx,
		tx:         tx,
		txOwned:    txOwned,
		l:          l,
		snapshotID: lock.NodeID,
		options:    options,
		ranges:     ranges,
	}, nil
}

type smartReader struct {
	ctx        context.Context
	yc         yt.Client
	tx         yt.Tx
	txOwned    bool
	l          log.Structured
	snapshotID yt.NodeID
	options    *yt.ReadTableOptions

	currentRangeIndex int
	currentRowIndex   *int64
	ranges            []ypath.Range

	r   yt.TableReader
	err error
}

func (r *smartReader) makeRange() *ypath.Rich {
	path := &ypath.Rich{
		Path:   r.snapshotID.YPath(),
		Ranges: []ypath.Range{r.ranges[r.currentRangeIndex]},
	}

	if r.currentRowIndex != nil {
		path.Ranges[0].Lower = &ypath.ReadLimit{RowIndex: r.currentRowIndex}
	}

	return path
}

func (r *smartReader) Next() bool {
	if r.currentRowIndex != nil {
		(*r.currentRowIndex)++
	}

	for {
		if r.r == nil {
			select {
			case <-r.tx.Finished():
				r.err = fmt.Errorf("read transaction aborted")
				return false
			default:
			}

			var err error
			r.r, err = r.tx.ReadTable(r.ctx, r.makeRange(), r.options)
			if err != nil {
				if yterrors.ContainsResolveError(err) {
					r.err = err
					return false
				}

				r.l.Warn("retrying error in smart reader", log.Error(err))
				// TODO(prime@): add backoff
				continue
			}
		}

		if r.r.Next() {
			if r.currentRowIndex == nil {
				rowIndex, ok := yt.StartRowIndex(r.r)
				if !ok {
					_ = r.r.Close()
					r.r = nil

					r.err = fmt.Errorf("reader does not support start_row_index")
					return false
				}
				r.currentRowIndex = &rowIndex
			}

			return true
		}

		if err := r.r.Err(); err != nil {
			_ = r.r.Close()
			r.r = nil

			r.l.Warn("retrying error in smart reader", log.Error(err))
			// TODO(prime@): add backoff
			continue
		}

		if r.currentRangeIndex+1 == len(r.ranges) {
			return false
		}

		r.currentRangeIndex++
		r.currentRowIndex = nil
		_ = r.r.Close()
		r.r = nil
	}
}

func (r *smartReader) Err() error {
	return r.err
}

func (r *smartReader) Scan(row any) error {
	if r.r == nil {
		return fmt.Errorf("Scan() called before Next()")
	}

	// Assume that Scan() does not perform any IO.
	return r.r.Scan(row)
}

func (r *smartReader) Close() error {
	var err error
	if r.r != nil {
		err = r.r.Close()
		r.r = nil
	}

	if r.txOwned {
		err = r.tx.Abort()
	}
	return err
}
