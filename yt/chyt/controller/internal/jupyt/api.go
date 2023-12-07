package jupyt

import (
	"context"
	"fmt"
	"net"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type GetEndpointResult struct {
	Address     string         `yson:"address"`
	OperationID yt.OperationID `yson:"operation_id"`
	JobID       yt.JobID       `yson:"job_id"`
}

func GetEndpoint(a *api.API, ctx context.Context, alias string) (result GetEndpointResult, err error) {
	briefInfo, err := a.GetBriefInfo(ctx, alias)
	if err != nil {
		return
	}

	opID := briefInfo.YTOperation.ID

	ytc := a.Ytc

	jobs, err := ytc.ListJobs(ctx, opID, &yt.ListJobsOptions{JobState: &yt.JobRunning})
	if err != nil {
		return
	}

	if len(jobs.Jobs) == 0 {
		err = yterrors.Err(fmt.Sprintf("no running jobs in operation %v", opID))
		return
	}

	address, _, err := net.SplitHostPort(jobs.Jobs[0].Address)
	if err != nil {
		return
	}

	result = GetEndpointResult{
		Address:     fmt.Sprintf("http://%v:%v", address, JupytPort),
		OperationID: opID,
		JobID:       jobs.Jobs[0].ID,
	}

	return
}
