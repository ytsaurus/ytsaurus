// Package migrate provides helper functions for creation and migration of dynamic YT tables.
package migrate

import (
	"context"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type retrySentinel struct{}

func (*retrySentinel) Error() string {
	return ""
}

// RetryConflict is a sentinel value that might be returned from ConflictFn.
var RetryConflict error = &retrySentinel{}

const defaultTabletWaitTimeout = time.Minute

func waitTabletState(ctx context.Context, yc yt.Client, path ypath.Path, state string) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, defaultTabletWaitTimeout)
		defer cancel()
	}

	return yt.PollMaster(ctx, yc, func() (stop bool, err error) {
		var currentState string
		err = yc.GetNode(ctx, path.Attr("tablet_state"), &currentState, nil)
		if err != nil {
			return
		}

		if currentState == state {
			stop = true
			return
		}

		return
	})
}

// MountAndWait mounts dynamic table and waits for a table to become mounted.
func MountAndWait(ctx context.Context, yc yt.Client, path ypath.Path) error {
	err := yc.MountTable(ctx, path, nil)
	if err != nil {
		return err
	}

	return waitTabletState(ctx, yc, path, yt.TabletMounted)
}

// FreezeAndWait freezes dynamic table and waits for a table to become frozen.
func FreezeAndWait(ctx context.Context, yc yt.Client, path ypath.Path) error {
	err := yc.FreezeTable(ctx, path, nil)
	if err != nil {
		return err
	}

	return waitTabletState(ctx, yc, path, yt.TabletFrozen)
}

// UnfreezeAndWait unfreezes dynamic table and waits for a table to become mounted.
func UnfreezeAndWait(ctx context.Context, yc yt.Client, path ypath.Path) error {
	err := yc.UnfreezeTable(ctx, path, nil)
	if err != nil {
		return err
	}

	return waitTabletState(ctx, yc, path, yt.TabletMounted)
}

// UnmountAndWait unmounts dynamic table and waits for a table to become unmounted.
func UnmountAndWait(ctx context.Context, yc yt.Client, path ypath.Path) error {
	err := yc.UnmountTable(ctx, path, nil)
	if err != nil {
		return err
	}

	return waitTabletState(ctx, yc, path, yt.TabletUnmounted)
}

// ConflictFn is function called from migration routine for table that already exists but got unexpected schema.
type ConflictFn func(path ypath.Path, actual, expected schema.Schema) error

// OnConflictDrop returns ConflictFn that will drop previous version for the table.
func OnConflictDrop(ctx context.Context, yc yt.Client) ConflictFn {
	return func(path ypath.Path, actual, expected schema.Schema) (err error) {
		if err = UnmountAndWait(ctx, yc, path); err != nil {
			return
		}

		if err = yc.RemoveNode(ctx, path, nil); err != nil {
			return
		}

		return RetryConflict
	}
}

var ErrConflict = xerrors.NewSentinel("detected schema conflict during migration")

// OnConflictFail is ConflictFn that will just return ErrConflict.
func OnConflictFail(path ypath.Path, actual, expected schema.Schema) (err error) {
	return ErrConflict.Wrap(yterrors.Err("schema differs",
		yterrors.Attr("actual_schema", actual),
		yterrors.Attr("expected_schema", expected)))
}

// OnConflictTryAlter returns ConflictFn that will try to alter previous version of the table.
func OnConflictTryAlter(ctx context.Context, yc yt.Client) ConflictFn {
	return func(path ypath.Path, actual, expected schema.Schema) (err error) {
		if err = UnmountAndWait(ctx, yc, path); err != nil {
			return
		}

		if err = yc.AlterTable(ctx, path, &yt.AlterTableOptions{
			Schema: &expected,
		}); err != nil {
			return
		}

		return RetryConflict
	}
}

type Table struct {
	Schema     schema.Schema
	Attributes map[string]interface{}
}

// EnsureTables creates and mounts dynamic tables.
//
// If table with a given path already exists, but have a different schema, onConflict handler is invoked.
func EnsureTables(
	ctx context.Context,
	yc yt.Client,
	tables map[ypath.Path]Table,
	onConflict ConflictFn,
) error {
	for path, table := range tables {
		var attrs struct {
			Schema      schema.Schema `yson:"schema"`
			TabletState string        `yson:"expected_tablet_state"`
		}

	retry:
		ok, err := yc.NodeExists(ctx, path, nil)
		if err != nil {
			return err
		}

		if !ok {
			attrs := make(map[string]interface{})
			for k, v := range table.Attributes {
				attrs[k] = v
			}

			attrs["dynamic"] = true
			attrs["schema"] = table.Schema

			_, err = yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
				Recursive:  true,
				Attributes: attrs,
			})

			if err != nil {
				return err
			}
		} else {
			opts := &yt.GetNodeOptions{Attributes: []string{
				"schema",
				"expected_tablet_state",
			}}

			if err := yc.GetNode(ctx, path.Attrs(), &attrs, opts); err != nil {
				return err
			}

			fixUniqueKeys := func(s schema.Schema) schema.Schema {
				if len(s.Columns) > 0 && s.Columns[0].SortOrder != schema.SortNone {
					s.UniqueKeys = true
				}

				return s
			}

			if !attrs.Schema.Equal(fixUniqueKeys(table.Schema)) {
				err = onConflict(path, attrs.Schema, table.Schema)
				if err == RetryConflict {
					goto retry
				}

				if err != nil {
					return err
				}
			}
		}

		if attrs.TabletState != yt.TabletMounted {
			if err := MountAndWait(ctx, yc, path); err != nil {
				return err
			}
		}
	}

	return nil
}
