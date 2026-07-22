package tech.ytsaurus.flow.service;

import java.nio.file.Files;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.jfr.JfrChunkLocator;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.rpc.CompanionServiceGrpc;
import tech.ytsaurus.flow.rpc.EResponseStatus;
import tech.ytsaurus.flow.rpc.TReqCompanionInfo;
import tech.ytsaurus.flow.rpc.TReqGetJfr;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.rpc.TResponseMetrics;
import tech.ytsaurus.flow.rpc.TRspCompanionInfo;
import tech.ytsaurus.flow.rpc.TRspGetJfr;
import tech.ytsaurus.flow.rpc.TRspProcessBatch;
import tech.ytsaurus.flow.rpc.TRspPutJob;
import tech.ytsaurus.flow.utils.YsonUtils;

/**
 * GRPC service for communication with worker.
 * Delegates request processing to {@link CompanionRequestProcessor}.
 */
public class CompanionService extends CompanionServiceGrpc.CompanionServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(CompanionService.class);
    private final CompanionRequestProcessor processor;

    public CompanionService(PipelineContextSnapshot context, JobContext jobContext) {
        super();
        this.processor = new CompanionRequestProcessor(context, jobContext);
    }

    @Override
    public void processBatch(TReqProcessBatch request, StreamObserver<TRspProcessBatch> responseObserver) {
        TRspProcessBatch response;
        try {
            var result = processor.processBatch(request);

            TRspProcessBatch.Builder responseBuilder = TRspProcessBatch.newBuilder();
            responseBuilder.setRequestId(request.getRequestId());
            responseBuilder.setJobId(request.getJobId());
            responseBuilder.setStatus(result.getStatus());

            if (result.getData() != null) {
                responseBuilder.setData(result.getData());
            }

            var responseMetrics = TResponseMetrics.newBuilder()
                    .setCpuTimeNs(result.getResourceStats().getCpuTime().toNanos())
                    .setAllocatedBytes(result.getResourceStats().getAllocatedBytes().toBytes())
                    .build();
            responseBuilder.setMetrics(responseMetrics);

            response = responseBuilder.build();
        } catch (Exception e) {
            log.error("Error processing batch", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(
                    "Error processing batch: " + e.getMessage()
            )));
            return;
        }
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
    public void getJfr(TReqGetJfr request, StreamObserver<TRspGetJfr> responseObserver) {
        TRspGetJfr response;
        try {
            JfrChunkLocator chunkLocator = new JfrChunkLocator();
            JfrChunkLocator.Result result = chunkLocator.findLatestCompleteChunk();

            TRspGetJfr.Builder responseBuilder = TRspGetJfr.newBuilder();

            switch (result) {
                case JfrChunkLocator.Result.Found found -> {
                    byte[] data = Files.readAllBytes(found.chunkPath());
                    responseBuilder.setStatus(EResponseStatus.RS_OK);
                    responseBuilder.setJfrData(UnsafeByteOperations.unsafeWrap(data));
                }
                case JfrChunkLocator.Result.NotFound notFound -> {
                    responseBuilder.setStatus(EResponseStatus.RS_ERROR);
                    responseBuilder.setErrorMessage(notFound.reason());
                }
                case JfrChunkLocator.Result.Error error -> {
                    responseBuilder.setStatus(EResponseStatus.RS_ERROR);
                    responseBuilder.setErrorMessage(error.reason());
                }
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
