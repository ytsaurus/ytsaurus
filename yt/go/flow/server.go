package flow

import (
	"context"
	"errors"
	"maps"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/flow/companion"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/ytlog"
)

const (
	ModeEnvVar   = "YT_FLOW_MODE"
	ConfigEnvVar = "YT_FLOW_COMPANION_CONFIG"

	// WorkerMode is the only mode a companion may run in.
	WorkerMode = "Worker"
)

var (
	// ErrInvalidConfig reports an environment a companion cannot serve in.
	ErrInvalidConfig = xerrors.NewSentinel("invalid companion config")

	// ErrServerRunning reports an attempt to configure a server that already serves.
	ErrServerRunning = xerrors.NewSentinel("server is already running")

	// ErrDuplicateComputation reports two computations registered under one id.
	ErrDuplicateComputation = xerrors.NewSentinel("duplicate computation")
)

// Config configures a companion process.
type Config struct {
	// Port is the port the companion serves on; 0 binds an ephemeral one.
	Port int
}

// LoadConfig reads companion configuration from the environment.
func LoadConfig() (Config, error) {
	mode, modeSet := os.LookupEnv(ModeEnvVar)
	raw, rawSet := os.LookupEnv(ConfigEnvVar)

	if !modeSet && !rawSet {
		return Config{}, nil
	}
	return ParseConfig(mode, []byte(raw))
}

// ParseConfig validates the mode and decodes YSON configuration.
func ParseConfig(mode string, raw []byte) (Config, error) {
	if mode == "" {
		return Config{}, xerrors.Errorf("flow: %w: %s is not set", ErrInvalidConfig, ModeEnvVar)
	}
	if mode != WorkerMode {
		return Config{}, xerrors.Errorf("flow: %w: companion started in mode %q, want %q",
			ErrInvalidConfig, mode, WorkerMode)
	}
	if len(raw) == 0 {
		return Config{}, xerrors.Errorf("flow: %w: %s is not set", ErrInvalidConfig, ConfigEnvVar)
	}

	var parsed rawConfig
	if err := yson.Unmarshal(raw, &parsed); err != nil {
		return Config{}, xerrors.Errorf("flow: %w: %s: %w", ErrInvalidConfig, ConfigEnvVar, err)
	}

	if err := validatePort("port", parsed.Port); err != nil {
		return Config{}, err
	}
	if parsed.CompanionProcessCount < 0 {
		return Config{}, xerrors.Errorf("flow: %w: companion_process_count is %d, want at least 0",
			ErrInvalidConfig, parsed.CompanionProcessCount)
	}

	return Config{
		Port: parsed.Port,
	}, nil
}

type rawConfig struct {
	Port                  int `yson:"port"`
	CompanionProcessCount int `yson:"companion_process_count"`
}

func validatePort(name string, port int) error {
	if port < 0 || port > math.MaxUint16 {
		return xerrors.Errorf("flow: %w: %s is %d, want 0 to %d",
			ErrInvalidConfig, name, port, math.MaxUint16)
	}
	return nil
}

// ServerOption configures a Server at construction.
type ServerOption func(*Server)

// WithLogger configures logging for request errors.
func WithLogger(logger log.Structured) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

// Server serves computations through CompanionService.
type Server struct {
	config      Config
	logger      log.Structured
	jobs        *jobCache
	cpuProfiler cpuProfiler
	memoryProbe memoryProbe

	mu            sync.Mutex
	computations  map[string]*Computation
	running       bool
	grpcServer    *grpc.Server
	listener      net.Listener
	cpuTracker    *cpuTracker
	memoryTracker *memoryTracker
}

// NewServer returns a server configured but not yet listening.
func NewServer(config Config, opts ...ServerOption) *Server {
	s := &Server{
		config:       config,
		logger:       ytlog.Must(),
		jobs:         newJobCache(),
		cpuProfiler:  runtimeCPUProfiler{},
		memoryProbe:  runtimeMemoryProbe{},
		computations: map[string]*Computation{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Config returns the server configuration.
func (s *Server) Config() Config {
	return s.config
}

// Register adds computations to the set this companion serves.
func (s *Server) Register(computations ...*Computation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return xerrors.Errorf("flow: register computations: %w", ErrServerRunning)
	}
	for _, computation := range computations {
		if _, ok := s.computations[computation.ID()]; ok {
			return xerrors.Errorf("flow: register computation %q: %w",
				computation.ID(), ErrDuplicateComputation)
		}
		s.computations[computation.ID()] = computation
	}
	return nil
}

// Start serves until Stop is called, and reports what stopped the server.
func (s *Server) Start() error {
	grpcServer, listener, err := s.listen()
	if err != nil {
		return err
	}
	return s.serve(grpcServer, listener)
}

// StartAsync starts serving in the background.
func (s *Server) StartAsync() error {
	grpcServer, listener, err := s.listen()
	if err != nil {
		return err
	}
	go func() {
		if err := s.serve(grpcServer, listener); err != nil {
			s.logger.Error("flow: companion server stopped", log.Error(err))
		}
	}()
	return nil
}

// Stop stops serving.
func (s *Server) Stop() {
	grpcServer, stopResources := s.detachGeneration(nil)
	if grpcServer != nil {
		grpcServer.Stop()
	}
	stopResources()
}

// detachGeneration clears the per-generation server state under the mutex and
// returns the detached gRPC server plus a function stopping the generation's
// resources. A non-nil match detaches only if it is still the current
// generation (the serve() self-exit path); Stop passes nil to detach whatever
// is current.
func (s *Server) detachGeneration(match *grpc.Server) (*grpc.Server, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if match != nil && s.grpcServer != match {
		return nil, func() {}
	}
	grpcServer := s.grpcServer
	cpuTracker := s.cpuTracker
	memoryTracker := s.memoryTracker
	s.grpcServer = nil
	s.listener = nil
	s.cpuTracker = nil
	s.memoryTracker = nil
	s.running = false

	return grpcServer, func() {
		if cpuTracker != nil {
			cpuTracker.Stop()
		}
		if memoryTracker != nil {
			memoryTracker.Stop()
		}
	}
}

// Port returns the port the server is listening on, or 0 before it starts.
func (s *Server) Port() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return 0
	}
	addr, ok := s.listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0
	}
	return addr.Port
}

// Running reports whether the server is serving.
func (s *Server) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.running
}

func (s *Server) listen() (*grpc.Server, net.Listener, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil, nil, xerrors.Errorf("flow: start companion server: %w", ErrServerRunning)
	}

	listener, err := net.Listen("tcp", net.JoinHostPort("::", strconv.Itoa(s.config.Port)))
	if err != nil {
		return nil, nil, xerrors.Errorf("flow: listen on port %d: %w", s.config.Port, err)
	}

	// Rejecting a worker-accepted batch by size would make it retry forever.
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
	)

	// Do not answer for jobs of a previous serving generation.
	s.jobs = newJobCache()

	var memoryTracker *memoryTracker
	if s.memoryProbe != nil {
		memoryTracker = newMemoryTracker(s.jobs, s.logger, s.memoryProbe, defaultMemoryTrackerConfig)
	}
	service := &companionService{
		computations: maps.Clone(s.computations),
		jobs:         s.jobs,
		logger:       s.logger,
		memory:       memoryTracker,
	}
	companion.RegisterCompanionServiceServer(grpcServer, service)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	var cpuTracker *cpuTracker
	if s.cpuProfiler != nil {
		cpuTracker = newCPUTracker(s.jobs, s.logger, s.cpuProfiler, cpuProfileWindow)
		cpuTracker.Start()
	}
	if memoryTracker != nil {
		memoryTracker.Start()
	}
	s.grpcServer = grpcServer
	s.listener = listener
	s.cpuTracker = cpuTracker
	s.memoryTracker = memoryTracker
	s.running = true

	return grpcServer, listener, nil
}

func (s *Server) serve(grpcServer *grpc.Server, listener net.Listener) error {
	err := grpcServer.Serve(listener)

	_, stopResources := s.detachGeneration(grpcServer)
	stopResources()

	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	if err != nil {
		return xerrors.Errorf("flow: serve companion service: %w", err)
	}
	return nil
}

type companionService struct {
	computations map[string]*Computation
	jobs         *jobCache
	logger       log.Structured
	memory       *memoryTracker
}

var _ companion.CompanionServiceServer = (*companionService)(nil)

// CompanionInfo reports the served computations.
func (s *companionService) CompanionInfo(
	_ context.Context,
	_ *companion.TReqCompanionInfo,
) (*companion.TRspCompanionInfo, error) {
	computations := make(map[string]companionComputationInfo, len(s.computations))
	for id, computation := range s.computations {
		computations[id] = companionComputationInfo{
			ComputationID:   id,
			ComputationType: computation.typ.String(),
		}
	}

	payload, err := yson.Marshal(companionInfo{Computations: computations})
	if err != nil {
		s.logger.Error("flow: companion info failed", log.Error(err))
		return &companion.TRspCompanionInfo{
			Payload: []byte{},
			Status:  companion.EResponseStatus_RS_ERROR.Enum(),
		}, nil
	}

	return &companion.TRspCompanionInfo{
		Payload: payload,
		Status:  companion.EResponseStatus_RS_OK.Enum(),
	}, nil
}

// PutJob caches a job configuration.
func (s *companionService) PutJob(
	ctx context.Context,
	req *companion.TReqPutJob,
) (*companion.TRspPutJob, error) {
	// The worker has abandoned this request; registering the job would leak
	// an entry nobody will remove.
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	var job *Job
	var err error
	jobID := misc.NewGUIDFromProto(req.GetJobId())
	requestID := misc.NewGUIDFromProto(req.GetRequestId())
	s.withJobResources(ctx, jobID, func(context.Context) {
		job, err = putJobFromProto(req)
		if err == nil {
			s.jobs.Put(job)
		}
	})
	if err != nil {
		s.logger.Error("flow: put job failed",
			log.String("job_id", misc.NewGUIDFromProto(req.GetJobId()).String()),
			log.String("computation_id", req.GetComputationId()),
			log.Error(err))
		return nil, status.Errorf(codes.Internal, "flow: put job failed: %v", err)
	}

	memoryUsage := int64(0)
	if s.memory != nil {
		memoryUsage = s.jobs.ResponseMemoryUsage(jobID, requestID, s.memory.Usage(jobID))
	}
	return &companion.TRspPutJob{
		RequestId: misc.NewProtoFromGUID(requestID),
		JobId:     misc.NewProtoFromGUID(misc.NewGUIDFromProto(req.GetJobId())),
		Metrics:   responseMetrics(0, memoryUsage),
		Status:    companion.EResponseStatus_RS_OK.Enum(),
	}, nil
}

// ProcessBatch runs a computation over one batch.
func (s *companionService) ProcessBatch(
	ctx context.Context,
	req *companion.TReqProcessBatch,
) (*companion.TRspProcessBatch, error) {
	// The worker has abandoned this request; the heal path must not register
	// a job nobody will remove.
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	var data *companion.TResponseData
	responseStatus := companion.EResponseStatus_RS_OK
	var panicTrace []byte
	jobID := misc.NewGUIDFromProto(req.GetJobId())

	var err error
	s.withJobResources(ctx, jobID, func(ctx context.Context) {
		err = func() (err error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					panicTrace = panicStack()
					err = xerrors.Errorf("flow: computation %q panicked: %v",
						req.GetComputationId(), recovered)
				}
			}()

			job, ok, err := s.job(req)
			if err != nil {
				return err
			}
			if !ok {
				responseStatus = companion.EResponseStatus_RS_JOB_NOT_FOUND
				return nil
			}

			computation, ok := s.computations[req.GetComputationId()]
			if !ok {
				return xerrors.Errorf("flow: computation %q is not served by this companion",
					req.GetComputationId())
			}

			requestRuntime, batch, err := processBatchFromProto(req, job)
			if err != nil {
				return err
			}

			results, err := computation.Process(ctx, requestRuntime, batch)
			if err != nil {
				return err
			}

			data, err = ResponseDataToProto(requestRuntime, results)
			return err
		}()
	})
	if err != nil {
		fields := []log.Field{
			log.String("request_id", misc.NewGUIDFromProto(req.GetRequestId()).String()),
			log.String("job_id", misc.NewGUIDFromProto(req.GetJobId()).String()),
			log.String("computation_id", req.GetComputationId()),
			log.Error(err),
		}
		if len(panicTrace) != 0 {
			fields = append(fields, log.String("stack_trace", string(panicTrace)))
		}
		s.logger.Error("flow: process batch failed", fields...)
		return nil, status.Errorf(codes.Internal, "flow: process batch failed: %v", err)
	}

	requestID := misc.NewGUIDFromProto(req.GetRequestId())
	cpuTime := int64(0)
	memoryUsage := int64(0)
	if responseStatus == companion.EResponseStatus_RS_OK {
		cpuTime = s.jobs.ResponseCPUTime(jobID, requestID)
	}
	if s.memory != nil {
		memoryUsage = s.memory.Usage(jobID)
		if responseStatus == companion.EResponseStatus_RS_OK {
			memoryUsage = s.jobs.ResponseMemoryUsage(jobID, requestID, memoryUsage)
		}
	}
	return &companion.TRspProcessBatch{
		RequestId: misc.NewProtoFromGUID(requestID),
		JobId:     misc.NewProtoFromGUID(misc.NewGUIDFromProto(req.GetJobId())),
		Data:      data,
		Metrics:   responseMetrics(cpuTime, memoryUsage),
		Status:    responseStatus.Enum(),
	}, nil
}

// RemoveJob removes a job from the registry; removal is idempotent.
func (s *companionService) RemoveJob(
	_ context.Context,
	req *companion.TReqRemoveJob,
) (*companion.TRspRemoveJob, error) {
	s.jobs.Delete(misc.NewGUIDFromProto(req.GetJobId()))

	return &companion.TRspRemoveJob{
		RequestId: req.GetRequestId(),
		JobId:     req.GetJobId(),
		Status:    companion.EResponseStatus_RS_OK.Enum(),
	}, nil
}

// ListJobs reports every job currently held by this process.
func (s *companionService) ListJobs(
	_ context.Context,
	req *companion.TReqListJobs,
) (*companion.TRspListJobs, error) {
	ids := s.jobs.ActiveIDs()
	jobIDs := make([]*misc.TGuid, 0, len(ids))
	for _, id := range ids {
		jobIDs = append(jobIDs, misc.NewProtoFromGUID(id))
	}

	return &companion.TRspListJobs{
		RequestId: req.GetRequestId(),
		JobIds:    jobIDs,
		ProcessId: proto.Int64(int64(os.Getpid())),
		Status:    companion.EResponseStatus_RS_OK.Enum(),
	}, nil
}

// ResourceExecute reports that companion resources are not implemented by the Go companion.
func (s *companionService) ResourceExecute(
	_ context.Context,
	req *companion.TReqResourceExecute,
) (*companion.TRspResourceExecute, error) {
	const message = "Companion resources are not supported by the Go companion"

	return &companion.TRspResourceExecute{
		RequestId: misc.NewProtoFromGUID(misc.NewGUIDFromProto(req.GetRequestId())),
		Status:    companion.EResourceExecuteStatus_RES_UNSUPPORTED.Enum(),
		Error: &misc.TError{
			Code:    proto.Int32(1),
			Message: proto.String(message),
		},
	}, nil
}

// GetJfr collects a Java Flight Recorder profile, which only the Java companion can do.
func (s *companionService) GetJfr(
	_ context.Context,
	_ *companion.TReqGetJfr,
) (*companion.TRspGetJfr, error) {
	const message = "GetJfr is supported by the Java companion only: " +
		"this companion is written in Go and records no flight recording"

	return &companion.TRspGetJfr{
		Status:       companion.EResponseStatus_RS_ERROR.Enum(),
		ErrorMessage: proto.String(message),
	}, nil
}

func (s *companionService) job(req *companion.TReqProcessBatch) (*Job, bool, error) {
	jobID := misc.NewGUIDFromProto(req.GetJobId())

	if req.GetJobInfo() == nil {
		job, ok := s.jobs.Get(jobID)
		return job, ok, nil
	}

	job, err := jobFromProto(jobID, req.GetComputationId(), req.GetJobInfo())
	if err != nil {
		return nil, false, err
	}
	s.jobs.Put(job)
	return job, true, nil
}

func (s *companionService) withJobResources(
	ctx context.Context,
	jobID guid.GUID,
	fn func(context.Context),
) {
	withJobCPU(ctx, jobID, func(ctx context.Context) {
		if s.memory == nil {
			fn(ctx)
			return
		}
		s.memory.WithJob(ctx, jobID, fn)
	})
}

type companionInfo struct {
	Computations map[string]companionComputationInfo `yson:"computations"`
}

type companionComputationInfo struct {
	ComputationID   string `yson:"computation_id"`
	ComputationType string `yson:"computation_type"`
}

func responseMetrics(cpuTime, memoryUsage int64) *companion.TResponseMetrics {
	return &companion.TResponseMetrics{
		AllocatedBytes: proto.Int64(memoryUsage),
		CpuTimeNs:      proto.Int64(cpuTime),
	}
}

func panicStack() []byte {
	buf := make([]byte, 8192)
	return buf[:runtime.Stack(buf, false)]
}
