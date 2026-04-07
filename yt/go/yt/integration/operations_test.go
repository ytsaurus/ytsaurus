package integration

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestOperation(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	inTable := tmpPath()
	outTable := tmpPath()

	for _, p := range []ypath.Path{inTable, outTable} {
		_, err := env.YT.CreateNode(ctx, p, yt.NodeTable, nil)
		require.NoError(t, err)
	}

	w, err := env.YT.WriteTable(ctx, inTable, nil)
	require.NoError(t, err)
	require.NoError(t, w.Write(map[string]any{"a": int64(1)}))
	require.NoError(t, w.Commit())

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{inTable},
		"output_table_paths": []ypath.Path{outTable},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "cat -",
		},
	}

	opID, err := env.YT.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)

	for {
		time.Sleep(time.Second)

		status, err := env.YT.GetOperation(ctx, opID, nil)
		require.NoError(t, err)

		if status.State == yt.StateCompleted {
			break
		}
	}

	r, err := env.YT.ReadTable(ctx, outTable, nil)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	var row any
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&row))
	require.Equal(t, map[string]any{"a": int64(1)}, row)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func init() {
	// Register jobs for Vanilla operation tests
	mapreduce.Register(&simpleVanillaJob{})
}

type simpleVanillaJob struct {
	mapreduce.Untyped
}

func (j *simpleVanillaJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	return nil
}

// setSpecParameter sets a parameter on UserScript by field name using reflection.
func setSpecParameter(s *spec.UserScript, fieldName string, value any) error {
	rv := reflect.ValueOf(s).Elem()
	field := rv.FieldByName(fieldName)
	if !field.IsValid() {
		return fmt.Errorf("field %s not found", fieldName)
	}

	// Convert value to pointer type if needed.
	val := value
	if field.Kind() == reflect.Ptr {
		// For pointer fields like *bool, wrap the value.
		v := reflect.New(field.Type().Elem())
		v.Elem().Set(reflect.ValueOf(value))
		val = v.Interface()
	}

	field.Set(reflect.ValueOf(val))
	return nil
}

func TestVanillaSpecParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		paramName   string
		paramVal    any
		structField string
	}{
		{
			name:        "enable_gpu_check true",
			paramName:   "enable_gpu_check",
			paramVal:    true,
			structField: "EnableGpuCheck",
		},
		{
			name:        "enable_gpu_check false",
			paramName:   "enable_gpu_check",
			paramVal:    false,
			structField: "EnableGpuCheck",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := yttest.New(t)

			ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
			ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()

			// Verify that the struct field exists.
			userScriptType := reflect.TypeOf((*spec.UserScript)(nil)).Elem()
			_, exists := userScriptType.FieldByName(tt.structField)
			require.True(t, exists, "Parameter %s not found in spec.UserScript struct", tt.structField)

			// Create Vanilla spec with parameter.
			s := spec.Vanilla().
				AddVanillaTask("test", 1)

			// Initialize mapper if nil and set parameter using reflection.
			if s.Mapper == nil {
				s.Mapper = &spec.UserScript{}
			}
			require.NoError(t, setSpecParameter(s.Mapper, tt.structField, tt.paramVal))

			// Create a simple job.
			job := &simpleVanillaJob{}

			op, err := env.MR.Vanilla(s, map[string]mapreduce.Job{"test": job})
			require.NoError(t, err)

			status, err := env.YT.GetOperation(ctx, op.ID(), nil)
			require.NoError(t, err)

			require.NotEmpty(t, status.ID)
			t.Logf("Vanilla operation started successfully with %s=%v", tt.paramName, tt.paramVal)
		})
	}
}

func TestOperationWithStderr(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	inTable := tmpPath()
	outTable := tmpPath()

	for _, p := range []ypath.Path{inTable, outTable} {
		_, err := env.YT.CreateNode(ctx, p, yt.NodeTable, nil)
		require.NoError(t, err)
	}

	w, err := env.YT.WriteTable(ctx, inTable, nil)
	require.NoError(t, err)
	require.NoError(t, w.Write(map[string]any{"a": int64(1)}))
	require.NoError(t, w.Commit())

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{inTable},
		"output_table_paths": []ypath.Path{outTable},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "echo hello >> /dev/stderr",
		},
	}

	opID, err := env.YT.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)
	for {
		time.Sleep(time.Second)
		status, err := env.YT.GetOperation(ctx, opID, nil)
		require.NoError(t, err)
		if status.State == yt.StateCompleted {
			break
		}
	}
	{
		taskName := "foo"
		jobs, err := env.YT.ListJobs(ctx, opID, &yt.ListJobsOptions{
			TaskName: &taskName,
		})
		require.NoError(t, err)
		require.Empty(t, jobs.Jobs)
	}
	taskName := "map"
	checkJob := func(job yt.JobStatus) {
		stderr, err := env.YT.GetJobStderr(ctx, opID, job.ID, nil)
		require.NoError(t, err)
		require.Equal(t, []byte("hello\n"), stderr)
		info, err := env.YT.GetJob(ctx, opID, job.ID, nil)
		require.NoError(t, err)
		require.Equal(t, job.ID, info.ID)
		require.Equal(t, job.Type, info.Type)
		require.Equal(t, job.TaskName, info.TaskName)
		require.Equal(t, taskName, job.TaskName)
	}
	jobs, err := env.YT.ListJobs(ctx, opID, nil)
	require.NoError(t, err)
	require.NotEmpty(t, jobs.Jobs)
	for _, job := range jobs.Jobs {
		checkJob(job)
	}
	jobs, err = env.YT.ListJobs(ctx, opID, &yt.ListJobsOptions{
		TaskName: &taskName,
	})
	require.NoError(t, err)
	require.NotEmpty(t, jobs.Jobs)
	for _, job := range jobs.Jobs {
		checkJob(job)
	}
}

func TestSuspendOperation(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	opSpec := map[string]any{
		"tasks": map[string]any{
			"main": map[string]any{
				"job_count": 1,
				"command":   "sleep 5m",
			},
		},
	}
	opID, err := env.YT.StartOperation(ctx, yt.OperationVanilla, opSpec, nil)
	require.NoError(t, err)

	// Wait for the operation to be in `running` state.
	for idx := 0; idx < 10; idx++ {
		opStatus, err := env.YT.GetOperation(ctx, opID, nil)
		require.NoError(t, err)
		if opStatus.State != yt.StateRunning {
			if idx == 9 {
				t.Fatalf("The operation is still in %s status", opStatus.State)
			}
			time.Sleep(3 * time.Second)
		} else {
			require.Equal(t, false, opStatus.Suspended)
			break
		}
	}

	// Suspend the operation.
	err = env.YT.SuspendOperation(ctx, opID, nil)
	require.NoError(t, err)

	// Check the operation status.
	opStatus, err := env.YT.GetOperation(ctx, opID, nil)
	require.NoError(t, err)
	require.Equal(t, yt.StateRunning, opStatus.State)
	require.Equal(t, true, opStatus.Suspended)

	err = env.YT.AbortOperation(ctx, opID, nil)
	require.NoError(t, err)
}

func TestListOperations(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	inTable := tmpPath()
	outTable := tmpPath()

	for _, p := range []ypath.Path{inTable, outTable} {
		_, err := env.YT.CreateNode(ctx, p, yt.NodeTable, nil)
		require.NoError(t, err)
	}

	w, err := env.YT.WriteTable(ctx, inTable, nil)
	require.NoError(t, err)
	require.NoError(t, w.Write(map[string]any{"a": int64(1)}))
	require.NoError(t, w.Commit())

	spec := map[string]any{
		"input_table_paths":  []ypath.Path{inTable},
		"output_table_paths": []ypath.Path{outTable},
		"mapper": map[string]any{
			"input_format":  "yson",
			"output_format": "yson",
			"command":       "echo hello >> /dev/stderr",
		},
	}

	opID, err := env.YT.StartOperation(ctx, yt.OperationMap, spec, nil)
	require.NoError(t, err)
	for {
		time.Sleep(time.Second)
		status, err := env.YT.GetOperation(ctx, opID, nil)
		require.NoError(t, err)
		if status.State == yt.StateCompleted {
			break
		}
	}
	ops, err := env.YT.ListOperations(ctx, nil)
	require.NoError(t, err)
	found := false
	for _, op := range ops.Operations {
		if op.ID == opID {
			found = true
		}
	}
	require.True(t, found, "Operation list must contain operation ID: %v", opID.String())
}

func TestListAllOperations(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
	ctx, cancel := context.WithTimeout(ctx, time.Minute*4)
	defer cancel()

	opAnnotation := guid.New().String()

	var opIDs []yt.OperationID
	for i := 0; i < 8; i++ {
		s := spec.Vanilla().
			AddVanillaTask("job", 1).
			AddAnnotations(map[string]any{
				"annotation": opAnnotation,
			})
		s.MaxFailedJobCount = 1

		op, err := env.MR.Vanilla(s, map[string]mapreduce.Job{"job": &HelloJob{}})
		require.NoError(t, err)
		require.NoError(t, op.Wait())

		opIDs = append(opIDs, op.ID())
	}

	operations, err := yt.ListAllOperations(ctx, env.YT, &yt.ListOperationsOptions{
		Limit:  ptr.Int(2),
		Filter: &opAnnotation,
	})
	require.NoError(t, err)

	var found []yt.OperationID
	for _, op := range operations {
		found = append(found, op.ID)
		require.Equal(t, op.RuntimeParameters.Annotations["annotation"], opAnnotation)
	}
	slices.Reverse(found)

	require.Equal(t, opIDs, found)
}

func TestListOperationEvents(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	opSpec := map[string]any{
		"tasks": map[string]any{
			"main": map[string]any{
				"job_count":    10,
				"command":      "echo hello >> /dev/stderr",
				"gang_options": map[string]any{},
			},
		},
	}

	opID, err := env.YT.StartOperation(ctx, yt.OperationVanilla, opSpec, nil)
	require.NoError(t, err)

	err = waitOpState(ctx, env.YT, opID, yt.StateCompleted)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	result, err := env.YT.ListOperationEvents(ctx, opID, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Events)

	for _, event := range result.Events {
		require.Equal(t, yt.IncarnationStarted, event.EventType)
		require.False(t, time.Time(event.Timestamp).IsZero())
	}
}

func TestListAllJobs(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	ctx := ctxlog.WithFields(context.Background(), log.String("subtest_name", t.Name()))
	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	s := spec.Vanilla().AddVanillaTask("job", 10)
	s.MaxFailedJobCount = 1

	op, err := env.MR.Vanilla(s, map[string]mapreduce.Job{"job": &HelloJob{}})
	require.NoError(t, err)
	require.NoError(t, op.Wait())

	jobs, err := yt.ListAllJobs(ctx, env.YT, op.ID(), &yt.ListJobsOptions{
		Limit: ptr.Int(2),
	})
	require.NoError(t, err)
	require.Len(t, jobs, 10)
}

type HelloJob struct {
	mapreduce.Untyped
}

func (c *HelloJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	_, err := fmt.Fprint(os.Stderr, "Hello!\n")
	return err
}

func init() {
	mapreduce.Register(&HelloJob{})
}
