// Package migrate provides helper functions for creation and migration of dynamic YT tables.
package migrate

import (
	"context"

	"a.yandex-team.ru/yt/go/yterrors"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type retrySentinel struct{}

func (*retrySentinel) Error() string {
	return ""
}

// RetryConflict is a sentinel value that might be returned from ConflictFn.
var RetryConflict error = &retrySentinel{}

// ensureTabletState updates dynamic table state to requested and waits for changes to take place.
func ensureTabletState(ctx context.Context, yc yt.Client, path ypath.Path, state string) error {
	var currentState, inProgressState string
	switch state {
	case yt.TabletMounted:
		inProgressState = yt.TabletMounting
	case yt.TabletUnmounted:
		inProgressState = yt.TabletUnmounting
	default:
		return xerrors.Errorf("tablet state %q is invalid", state)
	}

	err := yc.GetNode(ctx, path.Attr("tablet_state"), &currentState, nil)
	if err != nil {
		return err
	}

	if currentState != state && currentState != inProgressState {
		switch {
		case state == yt.TabletUnmounted:
			err := yc.UnmountTable(ctx, path, nil)
			if err != nil {
				return err
			}
		case state == yt.TabletMounted:
			err := yc.MountTable(ctx, path, nil)
			if err != nil {
				return err
			}
		}
	} else if currentState == state {
		return nil
	}

	return yt.PollMaster(ctx, yc, func() (stop bool, err error) {
		err = yc.GetNode(ctx, path.Attr("tablet_state"), &currentState, nil)
		if err != nil {
			return
		}

		if currentState == state {
			stop = true
			return
		}

		if currentState == yt.TabletTransient {
			return
		}

		if currentState != inProgressState {
			err = xerrors.Errorf("wrong tablet state on the master: actual=%q, expected=%q", currentState, inProgressState)
			return
		}

		return
	})
}

// MountAndWait mounts dynamic table and waits for a table to become mounted.
func MountAndWait(ctx context.Context, yc yt.Client, path ypath.Path) error {
	return ensureTabletState(ctx, yc, path, yt.TabletMounted)
}

// MountAndWait unmounts dynamic table and waits for a table to become unmounted.
func UnmountAndWait(ctx context.Context, yc yt.Client, path ypath.Path) error {
	return ensureTabletState(ctx, yc, path, yt.TabletUnmounted)
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

var ErrConflict = xerrors.New("detected schema conflict during migration")

// OnConflictFail is ConflictFn that will just return ErrConflict.
func OnConflictFail(path ypath.Path, actual, expected schema.Schema) (err error) {
	return ErrConflict
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
			Schema schema.Schema `yson:"schema"`
		}

	retry:
		if err := yc.GetNode(ctx, path.Attrs(), &attrs, &yt.GetNodeOptions{Attributes: []string{"schema"}}); err != nil {
			if yterrors.ContainsErrorCode(err, yterrors.CodeResolveError) {
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
			}
		} else {
			if !attrs.Schema.Equal(table.Schema.WithUniqueKeys()) {
				err = onConflict(path, attrs.Schema, table.Schema)
				if err == RetryConflict {
					goto retry
				}

				if err != nil {
					return err
				}
			}
		}

		if err := MountAndWait(ctx, yc, path); err != nil {
			return err
		}
	}

	return nil
}
