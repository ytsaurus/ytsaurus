package jupyt

import (
	"context"
	"fmt"
	"net"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type GetEndpointResult struct {
	Address     string         `yson:"address"`
	OperationID yt.OperationID `yson:"operation_id"`
	JobID       yt.JobID       `yson:"job_id"`
}

func getEndpointPortByJob(ctx context.Context, job yt.JobStatus, ytc yt.Client) (port int, err error) {
	path := ypath.Path("//sys/exec_nodes").
		Child(job.Address).
		Child("/orchid/exec_node/job_controller/active_jobs/").
		Child(job.ID.String()).
		Child("job_ports")

	var ports []int

	err = ytc.GetNode(
		ctx,
		path,
		&ports,
		nil,
	)
	if err != nil {
		return
	}

	if len(ports) != 1 {
		err = yterrors.Err(fmt.Sprintf("expected one port for job %v, got %v", job.ID.String(), ports))
		return
	}
	port = ports[0]
	return
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

	job := jobs.Jobs[0]

	address, _, err := net.SplitHostPort(job.Address)
	if err != nil {
		return
	}

	port, err := getEndpointPortByJob(ctx, job, ytc)
	if err != nil {
		return
	}

	result = GetEndpointResult{
		Address:     fmt.Sprintf("http://%v:%v", address, port),
		OperationID: opID,
		JobID:       jobs.Jobs[0].ID,
	}

	return
}
