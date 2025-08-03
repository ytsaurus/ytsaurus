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
		status, err := o.yc.GetOperation(o.ctx, o.opID, &yt.GetOperationOptions{
			Attributes: []string{"state", "result"},
		})
		if err != nil {
			return err
		}

		if status.State.IsFinished() {
			if status.Result.Error != nil && status.Result.Error.Code != 0 {
				return o.getOperationError(status.Result.Error)
			}

			return nil
		}

		time.Sleep(time.Second * 5)
	}
}
