package tech.ytsaurus.flow.service;

import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.Callable;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.TError;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.internal.utils.FailureCollector;
import tech.ytsaurus.flow.jfr.JfrChunkLocator;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.rpc.CompanionServiceGrpc;
import tech.ytsaurus.flow.rpc.EResponseStatus;
import tech.ytsaurus.flow.rpc.TReqCompanionInfo;
import tech.ytsaurus.flow.rpc.TReqGetJfr;
import tech.ytsaurus.flow.rpc.TReqListJobs;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.rpc.TReqRemoveJob;
import tech.ytsaurus.flow.rpc.TReqResourceExecute;
import tech.ytsaurus.flow.rpc.TResponseMetrics;
import tech.ytsaurus.flow.rpc.TRspCompanionInfo;
import tech.ytsaurus.flow.rpc.TRspGetJfr;
import tech.ytsaurus.flow.rpc.TRspListJobs;
import tech.ytsaurus.flow.rpc.TRspProcessBatch;
import tech.ytsaurus.flow.rpc.TRspPutJob;
import tech.ytsaurus.flow.rpc.TRspRemoveJob;
import tech.ytsaurus.flow.rpc.TRspResourceExecute;
import tech.ytsaurus.flow.utils.ProtoUtils;
import tech.ytsaurus.flow.utils.YsonUtils;

/**
 * GRPC service for communication with worker.
 * Delegates request processing to {@link CompanionRequestProcessor}.
 */
public class CompanionService extends CompanionServiceGrpc.CompanionServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(CompanionService.class);

    private final CompanionRequestProcessor processor;
    private final CompanionMetrics metrics;

    public CompanionService(PipelineContextSnapshot context, JobContext jobContext, MeterRegistry meterRegistry) {
        this(new CompanionRequestProcessor(context, jobContext), meterRegistry);
    }

    /**
     * Adapts a caller-owned processor to gRPC; does not construct its runtime dependencies.
     */
    public CompanionService(CompanionRequestProcessor processor, MeterRegistry meterRegistry) {
        this.processor = Objects.requireNonNull(processor);
        this.metrics = new CompanionMetrics(meterRegistry);
    }

    /**
     * Compatibility cleanup for standalone services. Server runtimes own their store explicitly.
     *
     * @return whether all resource hooks have finished; see ResourceStore.shutdown().
     */
    public boolean shutdown() {
        return processor.shutdown();
    }

    @Override
    public void processBatch(TReqProcessBatch request, StreamObserver<TRspProcessBatch> observer) {
        var measurement = metrics.startProcessBatch(request);
        if (Context.current().isCancelled()) {
            measurement.stop();
            cancel(observer);
            return;
        }
        respond("Error processing batch (ComputationId: " + request.getComputationId() + ")", observer,
                () -> FailureCollector.callWithCleanup(() -> {
                    if (request.hasJobInfo()) {
                        metrics.recordJobRecreation(request.getComputationId());
                    }
                    var result = processor.processBatch(request);
                    var response = TRspProcessBatch.newBuilder()
                            .setRequestId(request.getRequestId())
                            .setJobId(request.getJobId())
                            .setStatus(result.getStatus())
                            .setMetrics(responseMetrics(result.getResourceStats()));
                    if (result.getData() != null) {
                        response.setData(result.getData());
                        metrics.recordResponseStates(request.getComputationId(), result.getData());
                    }
                    return response.build();
                }, measurement::stop));
    }

    @Override
    public void companionInfo(TReqCompanionInfo request, StreamObserver<TRspCompanionInfo> observer) {
        respond("Error processing CompanionStatus request", observer, () -> {
            var result = processor.getCompanionInfo();
            return TRspCompanionInfo.newBuilder()
                    .setPayload(YsonUtils.protoFromYTree(result.getPayload()))
                    .setStatus(result.getStatus())
                    .build();
        });
    }

    @Override
    public void putJob(TReqPutJob request, StreamObserver<TRspPutJob> observer) {
        // An abandoned request must not register a job nobody will remove.
        if (Context.current().isCancelled()) {
            cancel(observer);
            return;
        }
        respond("Error processing PutJob request", observer, () -> {
            var result = processor.putJob(request);
            return TRspPutJob.newBuilder()
                    .setJobId(request.getJobId())
                    .setRequestId(request.getRequestId())
                    .setStatus(result.getStatus())
                    .setMetrics(responseMetrics(result.getResourceStats()))
                    .build();
        });
    }

    @Override
    public void removeJob(TReqRemoveJob request, StreamObserver<TRspRemoveJob> observer) {
        respond("Error processing RemoveJob request", observer, () -> TRspRemoveJob.newBuilder()
                .setRequestId(request.getRequestId())
                .setJobId(request.getJobId())
                .setStatus(processor.removeJob(request))
                .build());
    }

    @Override
    public void listJobs(TReqListJobs request, StreamObserver<TRspListJobs> observer) {
        respond("Error processing ListJobs request", observer, () -> {
            var response = TRspListJobs.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setProcessId(ProcessHandle.current().pid())
                    .setStatus(EResponseStatus.RS_OK);
            for (var jobId : processor.listJobs(request)) {
                response.addJobIds(ProtoUtils.toProto(jobId));
            }
            return response.build();
        });
    }

    @Override
    public void resourceExecute(TReqResourceExecute request, StreamObserver<TRspResourceExecute> observer) {
        respond("Error processing ResourceExecute request", observer, () -> {
            var outcome = processor.resourceExecute(request);
            var response = TRspResourceExecute.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setStatus(outcome.status());
            if (!outcome.errorMessage().isEmpty()) {
                response.setError(TError.newBuilder().setCode(1).setMessage(outcome.errorMessage()).build());
            }
            return response.build();
        });
    }

    @Override
    public void getJfr(TReqGetJfr request, StreamObserver<TRspGetJfr> observer) {
        respond("Error processing GetJfr request", observer, () -> {
            var result = new JfrChunkLocator().findLatestCompleteChunk();
            var response = TRspGetJfr.newBuilder();
            if (result instanceof JfrChunkLocator.Result.Found found) {
                response.setStatus(EResponseStatus.RS_OK)
                        .setJfrData(UnsafeByteOperations.unsafeWrap(Files.readAllBytes(found.chunkPath())));
            } else if (result instanceof JfrChunkLocator.Result.NotFound notFound) {
                response.setStatus(EResponseStatus.RS_ERROR).setErrorMessage(notFound.reason());
            } else if (result instanceof JfrChunkLocator.Result.Error error) {
                response.setStatus(EResponseStatus.RS_ERROR).setErrorMessage(error.reason());
            } else {
                throw new IllegalStateException("Unsupported JfrChunkLocator.Result type: " + result.getClass());
            }
            return response.build();
        });
    }

    private static TResponseMetrics responseMetrics(ResourceStats stats) {
        return TResponseMetrics.newBuilder()
                .setCpuTimeNs(stats.getCpuTime().toNanos())
                .setAllocatedBytes(stats.getAllocatedBytes().toBytes())
                .build();
    }

    private static void cancel(StreamObserver<?> observer) {
        observer.onError(Status.CANCELLED.withDescription("Request abandoned by the caller").asRuntimeException());
    }

    private static <T> void respond(String operation, StreamObserver<T> observer, Callable<T> body) {
        T response;
        try {
            response = body.call();
        } catch (Throwable error) {
            var failures = new FailureCollector();
            failures.add(error);
            boolean logged = failures.tryRun(() -> log.error(operation, error));
            boolean delivered = failures.tryRun(() -> observer.onError(Status.INTERNAL
                    .withDescription(TruncatedException.format(operation, error)).asRuntimeException()));
            if (error instanceof VirtualMachineError || !logged || !delivered) {
                failures.throwUncheckedIfAny();
            }
            return;
        }
        // Observer failures must not cause a second terminal callback.
        observer.onNext(response);
        observer.onCompleted();
    }
}
