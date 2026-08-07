package flow

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/flow/companion"
	"go.ytsaurus.tech/yt/go/yson"
)

var testRequestID = guid.FromHalves(0xdeadbeefcafebabe, 0x0123456789abcdef)

const fullConfigYSON = `{
	port = 4242;
	monitoring_port = 4243;
	companion_process_count = 8;
	cluster_url = "hahn";
	pipeline_path = "//home/flow/pipeline";
}`

func TestParseConfigReadsWorkerConfig(t *testing.T) {
	config, err := ParseConfig(WorkerMode, []byte(fullConfigYSON))
	require.NoError(t, err)

	require.Equal(t, 4242, config.Port)
}

func TestParseConfigIgnoresCompanionProcessCount(t *testing.T) {
	config, err := ParseConfig(WorkerMode, []byte(`{companion_process_count = 16}`))
	require.NoError(t, err)
	require.Equal(t, Config{}, config)
}

func TestParseConfigRejectsBadEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		raw  string
	}{
		{name: "mode unset", mode: "", raw: fullConfigYSON},
		{name: "mode is not Worker", mode: "Controller", raw: fullConfigYSON},
		{name: "config unset", mode: WorkerMode, raw: ""},
		{name: "config is not a map", mode: WorkerMode, raw: `["port"]`},
		{name: "config is not yson", mode: WorkerMode, raw: `{port = }`},
		{name: "port is negative", mode: WorkerMode, raw: `{port = -1}`},
		{name: "port is out of range", mode: WorkerMode, raw: `{port = 65536}`},
		{name: "process count is negative", mode: WorkerMode, raw: `{companion_process_count = -1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfig(tc.mode, []byte(tc.raw))
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestLoadConfigDefaultsOutsideWorker(t *testing.T) {
	unsetEnv(t, ModeEnvVar)
	unsetEnv(t, ConfigEnvVar)

	config, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, Config{}, config)
}

func TestLoadConfigEnforcesContractOncePartlySet(t *testing.T) {
	unsetEnv(t, ModeEnvVar)
	unsetEnv(t, ConfigEnvVar)
	t.Setenv(ModeEnvVar, WorkerMode)

	_, err := LoadConfig()
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestLoadConfigReadsEnvironment(t *testing.T) {
	t.Setenv(ModeEnvVar, WorkerMode)
	t.Setenv(ConfigEnvVar, fullConfigYSON)

	config, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, 4242, config.Port)
}

func TestServerStartAsyncBindsEphemeralPort(t *testing.T) {
	s := NewServer(Config{})
	require.Zero(t, s.Port())
	require.False(t, s.Running())

	require.NoError(t, s.StartAsync())
	t.Cleanup(s.Stop)

	require.True(t, s.Running())
	require.NotZero(t, s.Port())
}

func TestServerStartBlocksUntilStopped(t *testing.T) {
	s := NewServer(Config{})

	stopped := make(chan error, 1)
	go func() { stopped <- s.Start() }()
	require.Eventually(t, s.Running, 5*time.Second, time.Millisecond)

	s.Stop()

	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
	require.False(t, s.Running())
}

func TestServerRejectsSecondStart(t *testing.T) {
	s := NewServer(Config{})
	require.NoError(t, s.StartAsync())
	t.Cleanup(s.Stop)

	require.ErrorIs(t, s.StartAsync(), ErrServerRunning)
	require.ErrorIs(t, s.Start(), ErrServerRunning)
}

func TestServerStopIsIdempotent(t *testing.T) {
	s := NewServer(Config{})
	require.NoError(t, s.StartAsync())

	s.Stop()
	s.Stop()

	require.False(t, s.Running())
	require.Zero(t, s.Port())
}

func TestServerStopBeforeStart(t *testing.T) {
	s := NewServer(Config{})
	s.Stop()
	require.False(t, s.Running())
}

func TestServerRejectsDuplicateComputation(t *testing.T) {
	s := NewServer(Config{})
	require.NoError(t, s.Register(echoComputation()))
	require.ErrorIs(t, s.Register(echoComputation()), ErrDuplicateComputation)
}

func TestServerFreezesComputationsOnStart(t *testing.T) {
	s, client := startTestServer(t, echoComputation())

	require.ErrorIs(t, s.Register(NewRowComputation("late", noopRowFunc())), ErrServerRunning)

	require.Equal(t, map[string]string{"counter": "Transform"}, companionInfoOf(t, client))
}

func TestCompanionInfoReportsComputationTypes(t *testing.T) {
	_, client := startTestServer(t,
		echoComputation(),
		NewBatchSourceComputation("ingest", noopBatchFunc()),
	)

	require.Equal(t, map[string]string{
		"counter": "Transform",
		"ingest":  "Source",
	}, companionInfoOf(t, client))
}

func TestProcessBatchLoadsJobLazily(t *testing.T) {
	_, client := startTestServer(t, echoComputation())
	ctx := context.Background()

	req := processBatchRequest(t, nil)
	rsp, err := client.ProcessBatch(ctx, req)
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_JOB_NOT_FOUND, rsp.GetStatus())
	require.Empty(t, rsp.GetData().GetOutput())
	requireAnswered(t, req.GetRequestId(), req.GetJobId(), rsp.GetRequestId(), rsp.GetJobId(), rsp.GetMetrics())

	req.JobInfo = protoJobInfo(t)
	rsp, err = client.ProcessBatch(ctx, req)
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, rsp.GetStatus())
	require.Len(t, rsp.GetData().GetOutput(), 1)
	requireAnswered(t, req.GetRequestId(), req.GetJobId(), rsp.GetRequestId(), rsp.GetJobId(), rsp.GetMetrics())

	next := processBatchRequest(t, nil)
	rsp, err = client.ProcessBatch(ctx, next)
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, rsp.GetStatus())
}

func TestProcessBatchToleratesRetriedRequest(t *testing.T) {
	server, client := startTestServer(t, echoComputation())
	ctx := context.Background()

	req := processBatchRequest(t, protoJobInfo(t))
	server.jobs.Put(cachedTestJob(t, protoJobID))
	server.jobs.AddCPUTime(protoJobID, 123)

	first, err := client.ProcessBatch(ctx, req)
	require.NoError(t, err)
	server.jobs.AddCPUTime(protoJobID, 456)
	second, err := client.ProcessBatch(ctx, req)
	require.NoError(t, err)

	require.Equal(t, companion.EResponseStatus_RS_OK, second.GetStatus())
	require.True(t, proto.Equal(first.GetData(), second.GetData()))
	require.EqualValues(t, 123, first.GetMetrics().GetCpuTimeNs())
	require.EqualValues(t, 123, second.GetMetrics().GetCpuTimeNs())

	next := processBatchRequest(t, nil)
	next.RequestId = misc.NewProtoFromGUID(guid.FromHalves(8, 9))
	rsp, err := client.ProcessBatch(ctx, next)
	require.NoError(t, err)
	require.EqualValues(t, 456, rsp.GetMetrics().GetCpuTimeNs())
}

func TestProcessBatchLabelsUserCodeWithJobID(t *testing.T) {
	type labelResult struct {
		value string
		ok    bool
	}
	observed := make(chan labelResult, 1)
	labeled := NewRowComputation("counter", RowFunc(
		func(ctx context.Context, _ Runtime, _ ExtendedMessage, _ OutputCollector) error {
			value, ok := pprof.Label(ctx, cpuJobLabel)
			observed <- labelResult{value: value, ok: ok}
			return nil
		}))
	_, client := startTestServer(t, labeled)

	_, err := client.ProcessBatch(context.Background(), processBatchRequest(t, protoJobInfo(t)))
	require.NoError(t, err)
	result := <-observed
	require.True(t, result.ok)
	require.Equal(t, protoJobID.String(), result.value)
}

func TestPutJobCachesJobForLaterBatches(t *testing.T) {
	_, client := startTestServer(t, echoComputation())
	ctx := context.Background()

	put := &companion.TReqPutJob{
		RequestId:     misc.NewProtoFromGUID(testRequestID),
		JobId:         misc.NewProtoFromGUID(protoJobID),
		ComputationId: proto.String("counter"),
		JobInfo:       protoJobInfo(t),
	}
	rsp, err := client.PutJob(ctx, put)
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, rsp.GetStatus())
	requireAnswered(t, put.GetRequestId(), put.GetJobId(), rsp.GetRequestId(), rsp.GetJobId(), rsp.GetMetrics())

	batch, err := client.ProcessBatch(ctx, processBatchRequest(t, nil))
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, batch.GetStatus())
}

func TestPutJobReportsReplayableMemoryGauge(t *testing.T) {
	jobs := newJobCache()
	memory := newTestMemoryTracker(jobs, &fakeMemoryProbe{})
	memory.trackedJobs[protoJobID] = &trackedMemoryJob{usage: 123}
	service := &companionService{
		jobs:   jobs,
		logger: (&nop.Logger{}).Structured(),
		memory: memory,
	}
	req := &companion.TReqPutJob{
		RequestId:     misc.NewProtoFromGUID(testRequestID),
		JobId:         misc.NewProtoFromGUID(protoJobID),
		ComputationId: proto.String("counter"),
		JobInfo:       protoJobInfo(t),
	}

	rsp, err := service.PutJob(context.Background(), req)
	require.NoError(t, err)
	require.EqualValues(t, 123, rsp.GetMetrics().GetAllocatedBytes())

	memory.trackedJobs[protoJobID].usage = 456
	rsp, err = service.PutJob(context.Background(), req)
	require.NoError(t, err)
	require.EqualValues(t, 123, rsp.GetMetrics().GetAllocatedBytes())

	req.RequestId = misc.NewProtoFromGUID(guid.FromHalves(7, 8))
	rsp, err = service.PutJob(context.Background(), req)
	require.NoError(t, err)
	require.EqualValues(t, 456, rsp.GetMetrics().GetAllocatedBytes())
}

func TestProcessBatchJobNotFoundPreservesMemoryGauge(t *testing.T) {
	jobs := newJobCache()
	memory := newTestMemoryTracker(jobs, &fakeMemoryProbe{})
	memory.trackedJobs[protoJobID] = &trackedMemoryJob{usage: 123}
	service := &companionService{
		jobs:   jobs,
		logger: (&nop.Logger{}).Structured(),
		memory: memory,
	}

	rsp, err := service.ProcessBatch(context.Background(), processBatchRequest(t, nil))
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_JOB_NOT_FOUND, rsp.GetStatus())
	require.EqualValues(t, 123, rsp.GetMetrics().GetAllocatedBytes())
}

func TestPutJobReplacesCachedJob(t *testing.T) {
	observed := make(chan int64, 1)
	computation := NewRowComputation("counter", RowFunc(
		func(_ context.Context, rt Runtime, _ ExtendedMessage, _ OutputCollector) error {
			var windowSize int64
			if err := rt.DynamicParameters().Get("window_size", &windowSize); err != nil {
				return err
			}
			observed <- windowSize
			return nil
		}))
	_, client := startTestServer(t, computation)
	ctx := context.Background()

	for _, windowSize := range []int64{200, 300} {
		put, err := client.PutJob(ctx, &companion.TReqPutJob{
			RequestId:     misc.NewProtoFromGUID(testRequestID),
			JobId:         misc.NewProtoFromGUID(protoJobID),
			ComputationId: proto.String("counter"),
			JobInfo:       protoJobInfoWithWindowSize(t, windowSize),
		})
		require.NoError(t, err)
		require.Equal(t, companion.EResponseStatus_RS_OK, put.GetStatus())

		batch, err := client.ProcessBatch(ctx, processBatchRequest(t, nil))
		require.NoError(t, err)
		require.Equal(t, companion.EResponseStatus_RS_OK, batch.GetStatus())
		require.Equal(t, windowSize, <-observed)
	}
}

func TestPutJobReturnsInternalErrorOnUnparsableSpec(t *testing.T) {
	_, client := startTestServer(t, echoComputation())

	info := protoJobInfo(t)
	info.Spec = []byte(`{parameters = }`)

	rsp, err := client.PutJob(context.Background(), &companion.TReqPutJob{
		RequestId:     misc.NewProtoFromGUID(testRequestID),
		JobId:         misc.NewProtoFromGUID(protoJobID),
		ComputationId: proto.String("counter"),
		JobInfo:       info,
	})
	require.Nil(t, rsp)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "flow: put job failed")
}

func TestProcessBatchRejectsMalformedJobInfo(t *testing.T) {
	logger := newRecordingLogger()
	s := NewServer(Config{}, WithLogger(logger), withoutCPUAccounting(), withoutMemoryAccounting())
	require.NoError(t, s.Register(echoComputation()))
	require.NoError(t, s.StartAsync())
	t.Cleanup(s.Stop)
	client := dial(t, s.Port())

	info := protoJobInfo(t)
	info.Spec = []byte(`{parameters = }`)
	req := processBatchRequest(t, info)

	rsp, err := client.ProcessBatch(context.Background(), req)
	require.Nil(t, rsp)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "flow: process batch failed")

	require.Len(t, logger.messages(), 1)
	require.Contains(t, logger.messages()[0], "process batch failed")

	next, err := client.ProcessBatch(context.Background(), processBatchRequest(t, nil))
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_JOB_NOT_FOUND, next.GetStatus())
}

func TestProcessBatchReturnsInternalErrorOnUnknownComputation(t *testing.T) {
	_, client := startTestServer(t, echoComputation())

	req := processBatchRequest(t, protoJobInfo(t))
	req.ComputationId = proto.String("absent")

	rsp, err := client.ProcessBatch(context.Background(), req)
	require.Nil(t, rsp)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), `computation "absent"`)
}

func TestRemoveJobMakesJobUnknown(t *testing.T) {
	_, client := startTestServer(t, echoComputation())

	first, err := client.ProcessBatch(context.Background(), processBatchRequest(t, protoJobInfo(t)))
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, first.GetStatus())

	removeJob := func() *companion.TRspRemoveJob {
		rsp, err := client.RemoveJob(context.Background(), &companion.TReqRemoveJob{
			RequestId: misc.NewProtoFromGUID(testRequestID),
			JobId:     misc.NewProtoFromGUID(protoJobID),
		})
		require.NoError(t, err)
		return rsp
	}

	require.Equal(t, companion.EResponseStatus_RS_OK, removeJob().GetStatus())

	// The removed job is unknown until the worker heals it with job info.
	next, err := client.ProcessBatch(context.Background(), processBatchRequest(t, nil))
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_JOB_NOT_FOUND, next.GetStatus())

	// Removal is idempotent.
	require.Equal(t, companion.EResponseStatus_RS_OK, removeJob().GetStatus())

	// Job info heals the removed job: the entry is recreated, and the
	// worker's reconcile pass reclaims it if its job is gone.
	late, err := client.ProcessBatch(context.Background(), processBatchRequest(t, protoJobInfo(t)))
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, late.GetStatus())
}

func TestListJobsReportsRegistry(t *testing.T) {
	_, client := startTestServer(t, echoComputation())

	list := func() *companion.TRspListJobs {
		rsp, err := client.ListJobs(context.Background(), &companion.TReqListJobs{
			RequestId: misc.NewProtoFromGUID(testRequestID),
		})
		require.NoError(t, err)
		return rsp
	}

	rsp := list()
	require.Equal(t, companion.EResponseStatus_RS_OK, rsp.GetStatus())
	require.Empty(t, rsp.GetJobIds())
	require.EqualValues(t, os.Getpid(), rsp.GetProcessId())

	first, err := client.ProcessBatch(context.Background(), processBatchRequest(t, protoJobInfo(t)))
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, first.GetStatus())

	rsp = list()
	require.Len(t, rsp.GetJobIds(), 1)
	require.Equal(t, protoJobID, misc.NewGUIDFromProto(rsp.GetJobIds()[0]))

	_, err = client.RemoveJob(context.Background(), &companion.TReqRemoveJob{
		RequestId: misc.NewProtoFromGUID(testRequestID),
		JobId:     misc.NewProtoFromGUID(protoJobID),
	})
	require.NoError(t, err)
	require.Empty(t, list().GetJobIds())
}

func TestAbandonedRequestDoesNotRegisterJob(t *testing.T) {
	service := &companionService{
		jobs:   newJobCache(),
		logger: (&nop.Logger{}).Structured(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.PutJob(ctx, &companion.TReqPutJob{
		RequestId:     misc.NewProtoFromGUID(testRequestID),
		JobId:         misc.NewProtoFromGUID(protoJobID),
		ComputationId: proto.String("counter"),
		JobInfo:       protoJobInfo(t),
	})
	require.Error(t, err)
	require.Zero(t, service.jobs.Len())

	_, err = service.ProcessBatch(ctx, processBatchRequest(t, protoJobInfo(t)))
	require.Error(t, err)
	require.Zero(t, service.jobs.Len())
}

func TestProcessBatchSurvivesPanicInUserCode(t *testing.T) {
	panicking := NewRowComputation("counter", RowFunc(
		func(context.Context, Runtime, ExtendedMessage, OutputCollector) error {
			panic("user code exploded")
		}))
	logger := newRecordingLogger()
	s := NewServer(Config{}, WithLogger(logger), withoutCPUAccounting(), withoutMemoryAccounting())
	require.NoError(t, s.Register(panicking))
	require.NoError(t, s.StartAsync())
	t.Cleanup(s.Stop)
	client := dial(t, s.Port())

	req := processBatchRequest(t, protoJobInfo(t))
	rsp, err := client.ProcessBatch(context.Background(), req)
	require.Nil(t, rsp)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "user code exploded")
	require.NotContains(t, status.Convert(err).Message(), "goroutine")
	require.Len(t, logger.messages(), 1)

	require.Equal(t, map[string]string{"counter": "Transform"}, companionInfoOf(t, client))
}

func TestProcessBatchHandsRequestContextToUserCode(t *testing.T) {
	entered := make(chan struct{})
	observed := make(chan error, 1)
	blocking := NewRowComputation("counter", RowFunc(
		func(ctx context.Context, _ Runtime, _ ExtendedMessage, _ OutputCollector) error {
			close(entered)
			<-ctx.Done()
			observed <- ctx.Err()
			return ctx.Err()
		}))
	_, client := startTestServer(t, blocking)

	req := processBatchRequest(t, protoJobInfo(t))
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = client.ProcessBatch(ctx, req) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the computation was never called")
	}
	cancel()

	select {
	case err := <-observed:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the call did not reach user code")
	}
}

func TestProcessBatchReturnsHandlerErrorText(t *testing.T) {
	failing := NewRowComputation("counter", RowFunc(
		func(context.Context, Runtime, ExtendedMessage, OutputCollector) error {
			return errors.New("cannot update ledger")
		}))
	logger := newRecordingLogger()
	s := NewServer(Config{}, WithLogger(logger), withoutCPUAccounting(), withoutMemoryAccounting())
	require.NoError(t, s.Register(failing))
	require.NoError(t, s.StartAsync())
	t.Cleanup(s.Stop)

	client := dial(t, s.Port())
	rsp, err := client.ProcessBatch(context.Background(), processBatchRequest(t, protoJobInfo(t)))
	require.Nil(t, rsp)
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "cannot update ledger")

	require.Len(t, logger.messages(), 1)
	require.Contains(t, logger.messages()[0], "process batch failed")
}

type recordingLogger struct {
	log.Structured

	mu       sync.Mutex
	recorded []string
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{Structured: (&nop.Logger{}).Structured()}
}

func (l *recordingLogger) Error(msg string, _ ...log.Field) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.recorded = append(l.recorded, msg)
}

func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.recorded)
}

func TestServerServesHealthProtocol(t *testing.T) {
	s := NewServer(Config{})
	require.NoError(t, s.StartAsync())
	t.Cleanup(s.Stop)

	rsp, err := grpc_health_v1.NewHealthClient(dialConn(t, s.Port())).
		Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, rsp.GetStatus())
}

func TestGetJfrIsJavaOnly(t *testing.T) {
	_, client := startTestServer(t, echoComputation())

	rsp, err := client.GetJfr(context.Background(), &companion.TReqGetJfr{})
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_ERROR, rsp.GetStatus())
	require.Contains(t, rsp.GetErrorMessage(), "Java")
}

func startTestServer(t *testing.T, computations ...*Computation) (*Server, companion.CompanionServiceClient) {
	t.Helper()

	s := NewServer(Config{}, withoutCPUAccounting(), withoutMemoryAccounting())
	require.NoError(t, s.Register(computations...))
	require.NoError(t, s.StartAsync())
	t.Cleanup(s.Stop)

	return s, dial(t, s.Port())
}

func withoutCPUAccounting() ServerOption {
	return func(s *Server) {
		s.cpuProfiler = nil
	}
}

func withoutMemoryAccounting() ServerOption {
	return func(s *Server) {
		s.memoryProbe = nil
	}
}

func dial(t *testing.T, port int) companion.CompanionServiceClient {
	t.Helper()
	return companion.NewCompanionServiceClient(dialConn(t, port))
}

func dialConn(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(
		net.JoinHostPort("localhost", strconv.Itoa(port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func echoComputation() *Computation {
	return NewRowComputation("counter", RowFunc(
		func(_ context.Context, rt Runtime, _ ExtendedMessage, out OutputCollector) error {
			b, err := rt.MessageBuilder("clicks")
			if err != nil {
				return err
			}
			msg, err := b.Set("url", "seen").Finish()
			if err != nil {
				return err
			}
			out.AddMessage(msg)
			return nil
		}))
}

func noopRowFunc() RowFunc {
	return func(context.Context, Runtime, ExtendedMessage, OutputCollector) error { return nil }
}

func noopBatchFunc() BatchFunc {
	return func(context.Context, Runtime, []ExtendedMessage, OutputCollector) error { return nil }
}

func protoJobInfoWithWindowSize(t *testing.T, windowSize int64) *companion.TJobInfo {
	t.Helper()

	info := protoJobInfo(t)
	info.DynamicSpec = fmt.Appendf(nil, `{parameters = {window_size = %d}}`, windowSize)
	return info
}

func processBatchRequest(t *testing.T, info *companion.TJobInfo) *companion.TReqProcessBatch {
	t.Helper()
	return &companion.TReqProcessBatch{
		RequestId:     misc.NewProtoFromGUID(testRequestID),
		JobId:         misc.NewProtoFromGUID(protoJobID),
		ComputationId: proto.String("counter"),
		JobInfo:       info,
		Messages: []*companion.TReqProcessBatch_TExtendedMessage{
			clickMessage(t, "m-1", "http://a", batchKey(t, 17, "user-1")),
		},
	}
}

func requireAnswered(t *testing.T, reqID, jobID, gotReqID, gotJobID *misc.TGuid, metrics *companion.TResponseMetrics) {
	t.Helper()

	require.Equal(t, reqID.GetFirst(), gotReqID.GetFirst())
	require.Equal(t, reqID.GetSecond(), gotReqID.GetSecond())
	require.Equal(t, jobID.GetFirst(), gotJobID.GetFirst())
	require.Equal(t, jobID.GetSecond(), gotJobID.GetSecond())

	require.NotNil(t, metrics)
	require.Zero(t, metrics.GetCpuTimeNs())
	require.Zero(t, metrics.GetAllocatedBytes())
}

func companionInfoOf(t *testing.T, client companion.CompanionServiceClient) map[string]string {
	t.Helper()

	rsp, err := client.CompanionInfo(context.Background(), &companion.TReqCompanionInfo{})
	require.NoError(t, err)
	require.Equal(t, companion.EResponseStatus_RS_OK, rsp.GetStatus())

	var info struct {
		Computations map[string]struct {
			ComputationID   string `yson:"computation_id"`
			ComputationType string `yson:"computation_type"`
		} `yson:"computations"`
	}
	require.NoError(t, yson.Unmarshal(rsp.GetPayload(), &info))

	types := map[string]string{}
	for id, computation := range info.Computations {
		require.Equal(t, id, computation.ComputationID)
		types[id] = computation.ComputationType
	}
	return types
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()

	old, ok := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}
