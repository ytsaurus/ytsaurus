package mapreduce

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/cenkalti/backoff/v4"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Operation interface {
	ID() yt.OperationID

	// Wait polls until the operation finishes or the client context ends.
	Wait() error
}

const (
	operationPollInterval       = 5 * time.Second
	initialOperationPollTimeout = 30 * time.Second
	operationPollTimeout        = 10 * time.Minute
)

type operation struct {
	yc  yt.Client
	ctx context.Context

	opID yt.OperationID
}

func (o *operation) ID() yt.OperationID {
	return o.opID
}

var (
	failedJobLimitExceededRE = regexp.MustCompile("Failed jobs limit exceeded")
)

func (o *operation) getOperationError(opErr *yterrors.Error) error {
	if yterrors.ContainsMessageRE(opErr, failedJobLimitExceededRE) {
		result, err := o.yc.ListJobs(o.ctx, o.opID, &yt.ListJobsOptions{JobState: &yt.JobFailed})
		if err != nil {
			return yterrors.Err("unable to get list of failed jobs", opErr)
		}

		if len(result.Jobs) == 0 {
			return yterrors.Err("no failed jobs found", opErr)
		}

		job := result.Jobs[0]
		stderr, err := o.yc.GetJobStderr(o.ctx, o.opID, job.ID, nil)
		if err != nil {
			return yterrors.Err("unable to get job stderr", opErr)
		}

		return yterrors.Err("job failed",
			opErr,
			yterrors.Attr("stderr", string(stderr)))
	}

	return opErr
}

func (o *operation) Wait() error {
	for {
		status, err := o.getStatus(operationPollTimeout)
		if err != nil {
			return err
		}

		if status.State.IsFinished() {
			if status.Result.Error != nil && status.Result.Error.Code != 0 {
				return o.getOperationError(status.Result.Error)
			}

			return nil
		}

		select {
		case <-time.After(operationPollInterval):
		case <-o.ctx.Done():
			return o.ctx.Err()
		}
	}
}

func (o *operation) getStatus(timeout time.Duration) (*yt.OperationStatus, error) {
	options := &yt.GetOperationOptions{Attributes: []string{"state", "result"}}
	retryCtx, stop := context.WithTimeout(o.ctx, timeout)
	defer stop()

	policy := backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0))
	var delay time.Duration
	for requestTimeout := initialOperationPollTimeout; ; requestTimeout = timeout {
		requestCtx, cancel := context.WithTimeout(retryCtx, requestTimeout)
		status, err := o.yc.GetOperation(requestCtx, o.opID, options)
		cancel()
		if err == nil {
			return status, nil
		}
		if isRetriableOperationPollError(err) {
			select {
			case <-time.After(delay):
			case <-retryCtx.Done():
			}
			if retryCtx.Err() == nil {
				delay = policy.NextBackOff()
				continue
			}
			err = fmt.Errorf("reading status of operation %s: %w, last request: %w", o.opID, retryCtx.Err(), err)
		}
		if ctxErr := o.ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
}
