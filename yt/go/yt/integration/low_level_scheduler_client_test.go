package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestLowLevelSchedulerClient(t *testing.T) {
	suite := NewSuite(t)

	suite.RunClientTests(t, []ClientTest{
		{Name: "StartOperation", Test: suite.TestStartOperation},
		{Name: "AbortOperation", Test: suite.TestAbortOperation},
		{Name: "ResumeOperation", Test: suite.TestResumeOperation},
		{Name: "FailedOperation", Test: suite.TestFailedOperation},
		{Name: "CompleteOperation", Test: suite.TestCompleteOperation},
		{Name: "UpdateOperationParameters", Test: suite.TestUpdateOperationParameters},
		{Name: "ListOperations", Test: suite.TestListOperations},
		{Name: "ListJobs", Test: suite.TestListJobs},
		{Name: "GetOperationByAlias", Test: suite.TestGetOperationByAlias},
	})
}

func (s *Suite) TestStartOperation(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "cat -",
		},
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateCompleted)
	require.NoError(t, err)

	rows := scanRows(ctx, t, s.YT, out)
	require.Len(t, rows, 1)
	require.Equal(t, Row{"a": int64(1)}, rows[0])
}

func (s *Suite) TestAbortOperation(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "sleep 100",
		},
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateRunning)
	require.NoError(t, err)

	tctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	err = waitOpState(tctx, yc, opID, yt.StateCompleted)
	require.Error(t, err, "operation should not be completed after 10 seconds")

	err = yc.AbortOperation(ctx, opID, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateAborted)
	require.NoError(t, err)
}

func (s *Suite) TestResumeOperation(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "sleep 10 && cat -",
		},
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateRunning)
	require.NoError(t, err)

	err = yc.SuspendOperation(ctx, opID, &yt.SuspendOperationOptions{
		AbortRunningJobs: true,
	})
	require.NoError(t, err)

	time.Sleep(time.Second * 3)

	err = yc.ResumeOperation(ctx, opID, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateCompleted)
	require.NoError(t, err)

	rows := scanRows(ctx, t, s.YT, out)
	require.Len(t, rows, 1)
	require.Equal(t, Row{"a": int64(1)}, rows[0])
}

func (s *Suite) TestFailedOperation(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "run_constantly_failing_command",
		},
		"max_failed_job_count": 1,
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateFailed)
	require.NoError(t, err)
}

func (s *Suite) TestCompleteOperation(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "run_constantly_failing_command",
		},
		"max_failed_job_count": 10000,
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateRunning)
	require.NoError(t, err)

	time.Sleep(time.Second * 5)

	status, err := yc.GetOperation(ctx, opID, nil)
	require.NoError(t, err)
	require.Equal(t, yt.StateRunning, status.State)

	err = yc.CompleteOperation(ctx, opID, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateCompleted)
	require.NoError(t, err)
}

func (s *Suite) TestUpdateOperationParameters(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "sleep 100",
		},
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateRunning)
	require.NoError(t, err)

	annotation := guid.New().String()
	err = yc.UpdateOperationParameters(ctx, opID, map[string]any{
		"annotations": map[string]any{
			"test-annotations": annotation,
		},
	}, nil)
	require.NoError(t, err)

	status, err := yc.GetOperation(ctx, opID, nil)
	require.NoError(t, err)
	require.Equal(t, yt.StateRunning, status.State)
	require.Contains(t, status.RuntimeParameters.Annotations, "test-annotations")
	require.Equal(t, annotation, status.RuntimeParameters.Annotations["test-annotations"])

	err = yc.AbortOperation(ctx, opID, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateAborted)
	require.NoError(t, err)
}

func (s *Suite) TestListOperations(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	poolTree := "default"
	pool := "default"
	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "cat -",
		},
		"pool_trees": []string{poolTree},
		"pool":       pool,
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateCompleted)
	require.NoError(t, err)

	result, err := yc.ListOperations(ctx, &yt.ListOperationsOptions{PoolTree: &poolTree, Pool: &pool})
	require.NoError(t, err)

	opIDs := make([]yt.OperationID, 0, len(result.Operations))
	for _, op := range result.Operations {
		opIDs = append(opIDs, op.ID)
	}

	require.Contains(t, opIDs, opID)
}

func (s *Suite) TestListJobs(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "run_constantly_failing_command",
		},
		"max_failed_job_count": 1,
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateFailed)
	require.NoError(t, err)

	result, err := yc.ListJobs(ctx, opID, nil)
	require.NoError(t, err)
	require.Len(t, result.Jobs, 1)

	job := result.Jobs[0]

	require.Equal(t, string(yt.JobFailed), job.State)
	require.NotEmpty(t, job.Error.Message)
}

func (s *Suite) TestGetOperationByAlias(ctx context.Context, t *testing.T, yc yt.Client) {
	t.Parallel()

	in := makeTable(ctx, t, s.Env, []Row{{"a": int64(1)}})
	out := makeTable(ctx, t, s.Env, nil)

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{in},
		"output_table_paths": []ypath.Path{out},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "cat -",
		},
		"alias": "*test-alias",
	}

	opID, err := yc.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, yc, opID, yt.StateCompleted)
	require.NoError(t, err)

	status, err := yc.GetOperationByAlias(ctx, "*test-alias", &yt.GetOperationOptions{
		IncludeRuntime: ptr.Bool(true),
	})
	require.NoError(t, err)
	require.Equal(t, opID, status.ID)

	_, err = yc.GetOperationByAlias(ctx, "*fake-alias", &yt.GetOperationOptions{
		IncludeRuntime: ptr.Bool(true),
	})
	require.Error(t, err)
}

type Row map[string]any

func makeTable(ctx context.Context, t *testing.T, env *yttest.Env, rows []Row) ypath.Path {
	t.Helper()

	p := env.TmpPath()

	_, err := env.YT.CreateNode(ctx, p, yt.NodeTable, nil)
	require.NoError(t, err)

	w, err := env.YT.WriteTable(ctx, p, nil)
	require.NoError(t, err)
	for _, r := range rows {
		require.NoError(t, w.Write(r))
	}
	require.NoError(t, w.Commit())

	return p
}

func scanRows(ctx context.Context, t *testing.T, yc yt.Client, p ypath.Path) []Row {
	t.Helper()

	r, err := yc.ReadTable(ctx, p, nil)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	var rows []Row
	for r.Next() {
		var row Row
		require.NoError(t, r.Scan(&row))
		rows = append(rows, row)
	}

	require.NoError(t, r.Err())

	return rows
}

func waitOpState(ctx context.Context, yc yt.Client, id yt.OperationID, target yt.OperationState) error {
	for {
		time.Sleep(time.Second)

		status, err := yc.GetOperation(ctx, id, nil)
		if err != nil {
			return err
		}

		if status.State == target {
			break
		}
	}

	return nil
}
