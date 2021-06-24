package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yt/ytrpc"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/yttest"
	"github.com/stretchr/testify/require"
)

func TestLowLevelSchedulerClient(t *testing.T) {
	env := yttest.New(t)

	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	for _, tc := range []struct {
		name       string
		makeClient func() (yt.LowLevelSchedulerClient, error)
	}{
		{name: "http", makeClient: func() (yt.LowLevelSchedulerClient, error) {
			return ythttp.NewClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
		{name: "rpc", makeClient: func() (yt.LowLevelSchedulerClient, error) {
			return ytrpc.NewLowLevelSchedulerClient(&yt.Config{Proxy: os.Getenv("YT_PROXY"), Logger: ytlog.Must()})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := tc.makeClient()
			require.NoError(t, err)

			t.Run("StartOperation", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "cat -",
					},
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateCompleted)
				require.NoError(t, err)

				rows := scanRows(t, env, out)
				require.Len(t, rows, 1)
				require.Equal(t, Row{"a": int64(1)}, rows[0])
			})

			t.Run("AbortOperation", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "sleep 100",
					},
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateRunning)
				require.NoError(t, err)

				tctx, cancel := context.WithTimeout(env.Ctx, time.Second*10)
				defer cancel()
				err = waitOpState(tctx, client, opID, yt.StateCompleted)
				require.Error(t, err, "operation should not be completed after 10 seconds")

				err = client.AbortOperation(ctx, opID, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateAborted)
				require.NoError(t, err)
			})

			t.Run("ResumeOperation", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "sleep 10 && cat -",
					},
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateRunning)
				require.NoError(t, err)

				err = client.SuspendOperation(ctx, opID, &yt.SuspendOperationOptions{
					AbortRunningJobs: true,
				})
				require.NoError(t, err)

				time.Sleep(time.Second * 3)

				err = client.ResumeOperation(ctx, opID, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateCompleted)
				require.NoError(t, err)

				rows := scanRows(t, env, out)
				require.Len(t, rows, 1)
				require.Equal(t, Row{"a": int64(1)}, rows[0])
			})

			t.Run("FailedOperation", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "run_constantly_failing_command",
					},
					"max_failed_job_count": 1,
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateFailed)
				require.NoError(t, err)
			})

			t.Run("CompleteOperation", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "run_constantly_failing_command",
					},
					"max_failed_job_count": 10000,
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateRunning)
				require.NoError(t, err)

				time.Sleep(time.Second * 5)

				status, err := client.GetOperation(ctx, opID, nil)
				require.NoError(t, err)
				require.Equal(t, yt.StateRunning, status.State)

				err = client.CompleteOperation(ctx, opID, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateCompleted)
				require.NoError(t, err)
			})

			t.Run("UpdateOperationParameters", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "sleep 100",
					},
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateRunning)
				require.NoError(t, err)

				annotation := guid.New().String()
				err = client.UpdateOperationParameters(ctx, opID, map[string]interface{}{
					"annotations": map[string]interface{}{
						"test-annotations": annotation,
					},
				}, nil)
				require.NoError(t, err)

				status, err := client.GetOperation(ctx, opID, nil)
				require.NoError(t, err)
				require.Equal(t, yt.StateRunning, status.State)
				require.Contains(t, status.RuntimeParameters.Annotations, "test-annotations")
				require.Equal(t, annotation, status.RuntimeParameters.Annotations["test-annotations"])

				err = client.AbortOperation(ctx, opID, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateAborted)
				require.NoError(t, err)
			})

			t.Run("ListOperations", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "cat -",
					},
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateCompleted)
				require.NoError(t, err)

				result, err := client.ListOperations(ctx, nil)
				require.NoError(t, err)

				opIDs := make([]yt.OperationID, 0, len(result.Operations))
				for _, op := range result.Operations {
					opIDs = append(opIDs, op.ID)
				}

				require.Contains(t, opIDs, opID)
			})

			t.Run("ListJobs", func(t *testing.T) {
				t.Parallel()

				in := makeTable(t, env, []Row{{"a": int64(1)}})
				out := makeTable(t, env, nil)

				spec := map[string]interface{}{
					"input_table_paths":  []ypath.Path{in},
					"output_table_paths": []ypath.Path{out},
					"mapper": map[string]interface{}{
						"input_format":  "yson",
						"output_format": "yson",
						"command":       "echo hello >> /dev/stderr",
					},
				}

				opID, err := client.StartOperation(ctx, yt.OperationMap, spec, nil)
				require.NoError(t, err)

				err = waitOpState(ctx, client, opID, yt.StateCompleted)
				require.NoError(t, err)

				result, err := client.ListJobs(ctx, opID, nil)
				require.NoError(t, err)
				require.NotEmpty(t, result.Jobs)
			})
		})
	}
}

type Row map[string]interface{}

func makeTable(t *testing.T, env *yttest.Env, rows []Row) ypath.Path {
	t.Helper()

	p := env.TmpPath()

	_, err := env.YT.CreateNode(env.Ctx, p, yt.NodeTable, nil)
	require.NoError(t, err)

	w, err := env.YT.WriteTable(env.Ctx, p, nil)
	require.NoError(t, err)
	for _, r := range rows {
		require.NoError(t, w.Write(r))
	}
	require.NoError(t, w.Commit())

	return p
}

func scanRows(t *testing.T, env *yttest.Env, p ypath.Path) []Row {
	t.Helper()

	r, err := env.YT.ReadTable(env.Ctx, p, nil)
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

func waitOpState(ctx context.Context, yc yt.LowLevelSchedulerClient, id yt.OperationID, target yt.OperationState) error {
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
