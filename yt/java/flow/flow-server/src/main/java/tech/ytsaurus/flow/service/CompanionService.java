package tech.ytsaurus.flow.service;

import java.nio.file.Files;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.TError;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.jfr.JfrChunkLocator;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.rpc.CompanionServiceGrpc;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;
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
        super();
        this.processor = new CompanionRequestProcessor(context, jobContext);
        this.metrics = new CompanionMetrics(meterRegistry);
    }

    @Override
    public void processBatch(TReqProcessBatch request, StreamObserver<TRspProcessBatch> responseObserver) {
        var measurement = metrics.startProcessBatch(request);
        // An abandoned request must not register a job nobody will remove.
        if (Context.current().isCancelled()) {
            measurement.stop();
            responseObserver.onError(Status.CANCELLED
                    .withDescription("Request abandoned by the caller").asRuntimeException());
            return;
        }
        if (request.hasJobInfo()) {
            metrics.recordJobRecreation(request.getComputationId());
        }
        TRspProcessBatch response;
        try {
            var result = processor.processBatch(request);

            TRspProcessBatch.Builder responseBuilder = TRspProcessBatch.newBuilder();
            responseBuilder.setRequestId(request.getRequestId());
            responseBuilder.setJobId(request.getJobId());
            responseBuilder.setStatus(result.getStatus());

            if (result.getData() != null) {
                responseBuilder.setData(result.getData());
                metrics.recordResponseStates(request.getComputationId(), result.getData());
            }

            var responseMetrics = TResponseMetrics.newBuilder()
                    .setCpuTimeNs(result.getResourceStats().getCpuTime().toNanos())
                    .setAllocatedBytes(result.getResourceStats().getAllocatedBytes().toBytes())
                    .build();
            responseBuilder.setMetrics(responseMetrics);

            response = responseBuilder.build();
        } catch (Exception e) {
            measurement.stop();
            log.error("Error processing batch", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(
                    "Error processing batch: " + e.getMessage()
            )));
            return;
        }
        measurement.stop();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void companionInfo(TReqCompanionInfo request, StreamObserver<TRspCompanionInfo> responseObserver) {
        TRspCompanionInfo response;
        try {
            var result = processor.getCompanionInfo();

            response = TRspCompanionInfo.newBuilder()
                    .setPayload(YsonUtils.protoFromYTree(result.getPayload()))
                    .setStatus(result.getStatus())
                    .build();
        } catch (Exception e) {
            log.error("Error processing CompanionStatus request", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(
                    "Error processing CompanionStatus request: " + e.getMessage()
            )));
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putJob(TReqPutJob request, StreamObserver<TRspPutJob> responseObserver) {
        // An abandoned request must not register a job nobody will remove.
        if (Context.current().isCancelled()) {
            responseObserver.onError(Status.CANCELLED
                    .withDescription("Request abandoned by the caller").asRuntimeException());
            return;
        }
        TRspPutJob response;
        try {
            var result = processor.putJob(request);

            var responseBuilder = TRspPutJob.newBuilder()
                    .setJobId(request.getJobId())
                    .setRequestId(request.getRequestId())
                    .setStatus(result.getStatus());

            responseBuilder.setMetrics(TResponseMetrics.newBuilder()
                    .setCpuTimeNs(result.getResourceStats().getCpuTime().toNanos())
                    .setAllocatedBytes(result.getResourceStats().getAllocatedBytes().toBytes())
                    .build());

            response = responseBuilder.build();
        } catch (Exception e) {
            log.error("Error processing PutJob request", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(
                    "Error processing PutJob request: " + e.getMessage()
            )));
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void removeJob(TReqRemoveJob request, StreamObserver<TRspRemoveJob> responseObserver) {
        TRspRemoveJob response;
        try {
            var status = processor.removeJob(request);

            response = TRspRemoveJob.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setJobId(request.getJobId())
                    .setStatus(status)
                    .build();
        } catch (Exception e) {
            log.error("Error processing RemoveJob request", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(
                    "Error processing RemoveJob request: " + e.getMessage()
            )));
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void listJobs(TReqListJobs request, StreamObserver<TRspListJobs> responseObserver) {
        TRspListJobs response;
        try {
            var builder = TRspListJobs.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setProcessId(ProcessHandle.current().pid())
                    .setStatus(EResponseStatus.RS_OK);
            for (var jobId : processor.listJobs(request)) {
                builder.addJobIds(ProtoUtils.toProto(jobId));
            }
            response = builder.build();
        } catch (Exception e) {
            log.error("Error processing ListJobs request", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(
                    "Error processing ListJobs request: " + e.getMessage()
            )));
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void resourceExecute(
            TReqResourceExecute request,
            StreamObserver<TRspResourceExecute> responseObserver
    ) {
        var response = TRspResourceExecute.newBuilder()
                .setRequestId(request.getRequestId())
                .setStatus(EResourceExecuteStatus.RES_UNSUPPORTED)
                .setError(TError.newBuilder()
                        .setCode(1)
                        .setMessage("Companion resources are not supported by the Java companion")
                        .build())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getJfr(TReqGetJfr request, StreamObserver<TRspGetJfr> responseObserver) {
        TRspGetJfr response;
        try {
            JfrChunkLocator chunkLocator = new JfrChunkLocator();
            JfrChunkLocator.Result result = chunkLocator.findLatestCompleteChunk();

            TRspGetJfr.Builder responseBuilder = TRspGetJfr.newBuilder();

            if (result instanceof JfrChunkLocator.Result.Found found) {
                byte[] data = Files.readAllBytes(found.chunkPath());
                responseBuilder.setStatus(EResponseStatus.RS_OK);
                responseBuilder.setJfrData(UnsafeByteOperations.unsafeWrap(data));
            } else if (result instanceof JfrChunkLocator.Result.NotFound notFound) {
                responseBuilder.setStatus(EResponseStatus.RS_ERROR);
                responseBuilder.setErrorMessage(notFound.reason());
            } else if (result instanceof JfrChunkLocator.Result.Error error) {
                responseBuilder.setStatus(EResponseStatus.RS_ERROR);
                responseBuilder.setErrorMessage(error.reason());
            } else {
                throw new IllegalStateException("Unsupported JfrChunkLocator.Result type: "
                        + result.getClass().getName());
            }

            response = responseBuilder.build();
        } catch (Exception e) {
            log.error("Error processing GetJfr request", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(
                    "Error processing GetJfr request: " + e.getMessage()
            )));
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
