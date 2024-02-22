package ytlock

import (
	"context"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

// backOff is a back-off used in RunLocked.
var backOff = time.Second * 10

// ErrLockLost is an error propagated to notify in RunLocked
// when an acquired lock is lost.
var ErrLockLost = xerrors.NewSentinel("lock lost")

// Job is function cancellable via ctx.
type Job func(ctx context.Context) error

// Notify is a notify-on-error function.
//
// It receives a job error and backoff delay if the job failed.
type Notify func(error, time.Duration)

// RunLocked infinitely (with constant back-off) reacquires the lock and reruns the job.
//
// Stops when the job finishes with no error or when the context is closed.
//
// Calls notify on each failed attempt.
// In case of lost lock notify will have an error that wraps ErrLockLost.
func RunLocked(ctx context.Context, lock *Lock, job Job, notify Notify) error {
	for {
		err := lockAndRun(ctx, lock, job)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if !lock.IsLocked() {
				notify(ErrLockLost.Wrap(err), backOff)
			} else {
				notify(err, backOff)
			}

			if err := sleepWithContext(ctx, backOff); err != nil {
				return err
			}

			continue
		}

		return nil
	}
}

// lockAndRun acquires the lock and executes the job.
//
// Can be canceled via ctx.
func lockAndRun(ctx context.Context, lock *Lock, job Job) error {
	lost, err := lock.Acquire(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release(ctx) }()

	lockCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-lost:
			cancel()
		case <-lockCtx.Done():
		}
	}()

	return job(lockCtx)
}

// sleepWithContext sleeps for given duration.
//
// Can be canceled via ctx.
func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		break
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
