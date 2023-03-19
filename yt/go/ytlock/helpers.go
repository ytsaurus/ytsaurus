package ytlock

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// AbortExclusiveLock cancels transaction under exclusive lock specified by the path.
//
// There can not be more than one exclusive lock acquired for the path at the time.
//
// In case if node has not acquired an exclusive lock nil is returned.
func AbortExclusiveLock(ctx context.Context, yc yt.Client, path ypath.Path) error {
	var locks []*struct {
		TxID  yt.TxID      `yson:"transaction_id"`
		Mode  yt.LockMode  `yson:"mode"`
		State yt.LockState `yson:"state"`
	}
	if err := yc.GetNode(ctx, path.Attr("locks"), &locks, nil); err != nil {
		return err
	}
	for _, l := range locks {
		// There can only be single acquired exclusive transaction.
		if l.State == yt.LockAcquired && l.Mode == yt.LockExclusive {
			return yc.AbortTx(ctx, l.TxID, nil)
		}
	}
	return nil
}
