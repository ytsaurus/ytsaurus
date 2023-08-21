package yt

import (
	"context"
	"time"
)

const (
	TabletMounted    = "mounted"
	TabletMounting   = "mounting"
	TabletUnmounted  = "unmounted"
	TabletUnmounting = "unmounting"
	TabletFrozen     = "frozen"
	TabletFreezing   = "freezing"
	TabletTransient  = "transient"
)

// PollMaster abstracts away places where you would like to poll state change from the master.
//
// Calls poll in a loop until either, poll() returns an error, poll() signals to stop or ctx is canceled.
func PollMaster(ctx context.Context, yc Client, poll func() (stop bool, err error)) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		stop, err := poll()
		if err != nil {
			return err
		}

		if stop {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
