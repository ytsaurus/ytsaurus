package tech.ytsaurus.flow.service;

import java.util.Objects;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.internal.request.mapper.JobProtoMapper;
import tech.ytsaurus.flow.internal.request.mapper.RequestProtoMapper;
import tech.ytsaurus.flow.internal.request.mapper.ResponseProtoMapper;
import tech.ytsaurus.flow.internal.utils.FailureCollector;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.request.RequestContext;
import tech.ytsaurus.flow.request.ResponseContext;
import tech.ytsaurus.flow.rpc.EResponseStatus;
import tech.ytsaurus.flow.rpc.TJobInfo;
import tech.ytsaurus.flow.rpc.TReqListJobs;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.rpc.TReqRemoveJob;
import tech.ytsaurus.flow.rpc.TReqResourceExecute;
import tech.ytsaurus.flow.rpc.TResponseData;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Core request processor for Companion service operations.
 * This class contains the pure request processing logic,
 * separated from gRPC transport concerns to enable testing and benchmarking
 * without starting a gRPC server.
 */
public class CompanionRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(CompanionRequestProcessor.class);

    private final PipelineContextSnapshot pipelineContext;
    private final JobContext jobContext;
    private final ExecutionMeter executionMeter;
    private final ResourceStore resourceStore;

    private final RequestProtoMapper requestMapper;
    private final ResponseProtoMapper responseMapper;
    private final JobProtoMapper jobMapper;
    private final JobSpecValidator jobSpecValidator;

    public CompanionRequestProcessor(PipelineContextSnapshot pipelineContext, JobContext jobContext) {
        this(pipelineContext, jobContext, JobSpecValidator.NOOP);
    }

    public CompanionRequestProcessor(
            PipelineContextSnapshot pipelineContext,
            JobContext jobContext,
            JobSpecValidator jobSpecValidator
    ) {
        this(
                pipelineContext,
                jobContext,
                new ResourceStore(Objects.requireNonNull(pipelineContext).getResourceFactories()),
                jobSpecValidator
        );
    }

    /**
     * Processes requests using the resource store owned by the caller's runtime or test harness.
     */
    public CompanionRequestProcessor(
            PipelineContextSnapshot pipelineContext,
            JobContext jobContext,
            ResourceStore resourceStore
    ) {
        this(pipelineContext, jobContext, resourceStore, JobSpecValidator.NOOP);
    }

    public CompanionRequestProcessor(
            PipelineContextSnapshot pipelineContext,
            JobContext jobContext,
            ResourceStore resourceStore,
            JobSpecValidator jobSpecValidator
    ) {
        this.jobSpecValidator = Objects.requireNonNull(jobSpecValidator, "jobSpecValidator");
        this.pipelineContext = Objects.requireNonNull(pipelineContext, "pipelineContext must not be null");
        this.jobContext = Objects.requireNonNull(jobContext, "jobContext must not be null");
        this.executionMeter = new ExecutionMeter();
        this.resourceStore = Objects.requireNonNull(resourceStore);

        var streamContext = pipelineContext.getStreamContext();
        this.requestMapper = new RequestProtoMapper(streamContext);
        this.responseMapper = new ResponseProtoMapper();
        this.jobMapper = new JobProtoMapper(streamContext);
    }

    /**
     * Returns the process-wide store of the companion-hosted resources.
     *
     * @return the resource store
     */
    public ResourceStore getResourceStore() {
        return resourceStore;
    }

    /**
     * Releases the resources this companion runtime hosts. Idempotent.
     *
     * @return whether no batch lease outlives the call; see {@link ResourceStore#shutdown()}.
     */
    public boolean shutdown() {
        return resourceStore.shutdown();
    }

    /**
     * Process a batch request.
     *
     * @param request The batch processing request
     * @return Result containing status, response data, and resource statistics
     * @throws Exception if processing fails
     */
    public ProcessBatchResult processBatch(TReqProcessBatch request) throws Exception {
        var requestId = ProtoUtils.fromProto(request.getRequestId());
        var jobId = ProtoUtils.fromProto(request.getJobId());
        String computationId = request.getComputationId();

        log.debug("Processing batch: (RequestId: {}, JobId: {}, ComputationId: {}, HasJobInfo: {})",
                requestId, jobId, computationId, request.hasJobInfo());

        var measured = executionMeter.measure(() -> processBatchData(request, jobId, computationId));
        var stats = measured.stats();
        log.debug("Processed batch: (RequestId: {}, JobId: {}, AllocatedBytes: {}, CpuTime: {})",
                requestId, jobId, stats.getAllocatedBytes(), stats.getCpuTime());
        var result = measured.value();
        return new ProcessBatchResult(result.status(), result.data(), stats);
    }

    private BatchOutput processBatchData(TReqProcessBatch request, GUID jobId, String computationId) throws Exception {
        Job job = retrieveOrCreateJob(jobId, computationId, request.hasJobInfo() ? request.getJobInfo() : null);
        if (job == null) {
            return new BatchOutput(EResponseStatus.RS_JOB_NOT_FOUND, null);
        }
        ResourceLease lease = resourceStore.acquire(job.getCompanionResources());
        if (lease == null) {
            return new BatchOutput(EResponseStatus.RS_RESOURCE_NOT_INITIALIZED, null);
        }
        return FailureCollector.callWithCleanup(() -> {
            Computation computation = retrieveComputation(computationId);
            RequestContext requestCtx = requestMapper.fromProto(request, job, lease.resources());
            if (log.isTraceEnabled()) {
                log.trace("Request context: {}", requestCtx);
            }
            ResponseContext responseCtx = computation.doProcess(requestCtx);
            if (log.isTraceEnabled()) {
                log.trace("Processed response context: {}", responseCtx);
            }
            return new BatchOutput(EResponseStatus.RS_OK,
                    responseMapper.toProto(responseCtx, requestCtx.getStreamSpecs()));
        }, lease::close);
    }

    /**
     * Process a PutJob request.
     *
     * @param request The put job request
     * @return Result containing status and resource statistics
     * @throws Exception if processing fails
     */
    public PutJobResult putJob(TReqPutJob request) throws Exception {
        Objects.requireNonNull(request, "request must not be null");

        var requestId = ProtoUtils.fromProto(request.getRequestId());
        var jobId = ProtoUtils.fromProto(request.getJobId());

        log.debug("Processing PutJob: (RequestId: {}, JobId: {})", requestId, jobId);

        var measured = executionMeter.measure(() -> {
            jobSpecValidator.validate(request.getComputationId(), request.getJobInfo());
            var job = jobMapper.fromProto(request);
            jobContext.putJob(jobId, job);
            return EResponseStatus.RS_OK;
        });

        log.debug("Processed PutJob: (JobId: {})", jobId);

        return new PutJobResult(measured.value(), measured.stats());
    }

    /**
     * Process a RemoveJob request. Removal is idempotent: unknown ids are ignored.
     *
     * @param request The remove job request
     * @return The response status (always {@code RS_OK})
     */
    public EResponseStatus removeJob(TReqRemoveJob request) {
        Objects.requireNonNull(request, "request must not be null");

        var requestId = ProtoUtils.fromProto(request.getRequestId());
        var jobId = ProtoUtils.fromProto(request.getJobId());

        log.debug("Processing RemoveJob: (RequestId: {}, JobId: {})", requestId, jobId);

        jobContext.removeJob(jobId);
        return EResponseStatus.RS_OK;
    }

    /**
     * Process a ListJobs request.
     *
     * @param request The list jobs request
     * @return the ids of every job registered in this process
     */
    public Set<GUID> listJobs(TReqListJobs request) {
        Objects.requireNonNull(request, "request must not be null");
        return jobContext.listJobIds();
    }

    /**
     * Process a ResourceExecute request.
     *
     * <p>User-code failures come back in-band via the outcome status; exceptions escape only on
     * companion bugs.
     *
     * @param request The resource execute request
     * @return the command outcome carrying the in-band status and error message
     */
    public ExecuteOutcome resourceExecute(TReqResourceExecute request) {
        Objects.requireNonNull(request, "request must not be null");

        var requestId = ProtoUtils.fromProto(request.getRequestId());
        log.debug(
                "Processing ResourceExecute: (RequestId: {}, ResourceId: {}, Command: {})",
                requestId,
                request.getResourceId(),
                request.getCommand()
        );
        var argument = request.hasArgument() ? request.getArgument() : null;
        return resourceStore.execute(request.getResourceId(), request.getCommand().getNumber(), argument);
    }

    /**
     * Get companion information.
     *
     * @return Result containing status and pipeline context as YTree
     */
    public CompanionInfoResult getCompanionInfo() {
        log.debug("Processing CompanionInfo request");

        YTreeNode contextYTree = pipelineContext.toYTree();

        log.debug("CompanionInfo Context: {}", contextYTree);
        log.debug("Processed CompanionInfo");

        return new CompanionInfoResult(EResponseStatus.RS_OK, contextYTree);
    }

    private @Nullable Job retrieveOrCreateJob(GUID jobId, String computationId, @Nullable TJobInfo jobInfo) {
        if (jobInfo != null) {
            jobSpecValidator.validate(computationId, jobInfo);
            Job job = jobMapper.fromProto(jobId, computationId, jobInfo);
            jobContext.putJob(jobId, job);
            return job;
        }
        return jobContext.getJob(jobId);
    }

    /**
     * Retrieves a computation by ID, throwing an exception if not found.
     *
     * @param computationId The computation identifier
     * @return The computation
     * @throws IllegalArgumentException if computation is not found
     */
    private Computation retrieveComputation(String computationId) {
        Computation computation = pipelineContext.getComputation(computationId);
        if (computation == null) {
            throw new IllegalArgumentException(
                    "Computation not found: (ComputationId: %s)".formatted(computationId)
            );
        }
        return computation;
    }

    private record BatchOutput(EResponseStatus status, @Nullable TResponseData data) {
    }

    /**
     * Result of processing a batch request.
     */
    public static class ProcessBatchResult {
        private final EResponseStatus status;
        private final @Nullable TResponseData data;
        private final ResourceStats resourceStats;

        public ProcessBatchResult(EResponseStatus status, @Nullable TResponseData data, ResourceStats resourceStats) {
            this.status = status;
            this.data = data;
            this.resourceStats = resourceStats;
        }

        public EResponseStatus getStatus() {
            return status;
        }

        public @Nullable TResponseData getData() {
            return data;
        }

        public ResourceStats getResourceStats() {
            return resourceStats;
        }
    }

    /**
     * Result of processing a PutJob request.
     */
    public static class PutJobResult {
        private final EResponseStatus status;
        private final ResourceStats resourceStats;

        public PutJobResult(EResponseStatus status, ResourceStats resourceStats) {
            this.status = status;
            this.resourceStats = resourceStats;
        }

        public EResponseStatus getStatus() {
            return status;
        }

        public ResourceStats getResourceStats() {
            return resourceStats;
        }
    }

    /**
     * Result of processing a CompanionInfo request.
     */
    public static class CompanionInfoResult {
        private final EResponseStatus status;
        private final YTreeNode payload;

        public CompanionInfoResult(EResponseStatus status, YTreeNode payload) {
            this.status = status;
            this.payload = payload;
        }

        public EResponseStatus getStatus() {
            return status;
        }

        public YTreeNode getPayload() {
            return payload;
        }
    }
}
