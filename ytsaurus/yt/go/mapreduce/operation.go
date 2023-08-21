package mapreduce

import (
	"context"
	"regexp"
	"time"

	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type Operation interface {
	ID() yt.OperationID

	Wait() error
}

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

func (o *operation) getOperationError(status *yt.OperationStatus) error {
	innerErr := status.Result.Error

	if yterrors.ContainsMessageRE(status.Result.Error, failedJobLimitExceededRE) {
		result, err := o.yc.ListJobs(o.ctx, o.opID, &yt.ListJobsOptions{JobState: &yt.JobFailed})
		if err != nil {
			return yterrors.Err("unable to get list of failed jobs", innerErr)
		}

		if len(result.Jobs) == 0 {
			return yterrors.Err("no failed jobs found", innerErr)
		}

		job := result.Jobs[0]
		stderr, err := o.yc.GetJobStderr(o.ctx, o.opID, job.ID, nil)
		if err != nil {
			return yterrors.Err("unable to get job stderr", innerErr)
		}

		return yterrors.Err("job failed",
			innerErr,
			yterrors.Attr("stderr", string(stderr)))
	}

	return status.Result.Error
}

func (o *operation) Wait() error {
	for {
		status, err := o.yc.GetOperation(o.ctx, o.opID, nil)
		if err != nil {
			return err
		}

		if status.State.IsFinished() {
			if status.Result.Error != nil && status.Result.Error.Code != 0 {
				return o.getOperationError(status)
			}

			return nil
		}

		time.Sleep(time.Second * 5)
	}
}
