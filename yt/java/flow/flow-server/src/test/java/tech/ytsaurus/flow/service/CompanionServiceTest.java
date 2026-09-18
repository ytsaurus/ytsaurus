package tech.ytsaurus.flow.service;

import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.rpc.EResourceCommand;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;
import tech.ytsaurus.flow.rpc.TReqListJobs;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.rpc.TReqResourceExecute;
import tech.ytsaurus.flow.rpc.TRspListJobs;
import tech.ytsaurus.flow.rpc.TRspProcessBatch;
import tech.ytsaurus.flow.rpc.TRspPutJob;
import tech.ytsaurus.flow.rpc.TRspResourceExecute;
import tech.ytsaurus.flow.testutils.ProtobufRequestBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompanionService} error surfacing in ProcessBatch.
 */
class CompanionServiceTest {

    private static final String COMPUTATION_ID = ProtobufRequestBuilder.COMPUTATION_ID;

    private static CompanionService serviceWithFailure(RuntimeException failure) {
        return serviceThrowing(() -> {
            throw failure;
        });
    }

    private static CompanionService serviceWithFailure(Error failure) {
        return serviceThrowing(() -> {
            throw failure;
        });
    }

    private static CompanionService serviceThrowing(Runnable failure) {
        var pipelineContext = new PipelineContext();
        pipelineContext.registerComputation(Computation.builder()
                .setComputationId(COMPUTATION_ID)
                .setProcessFunction(new RowFunction() {
                    @Override
                    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
                        failure.run();
                    }
                })
                .build());
        return new CompanionService(
                new PipelineContextSnapshot(pipelineContext),
                new JobContext(),
                new SimpleMeterRegistry()
        );
    }

    private static TReqProcessBatch createRequest() {
        return new ProtobufRequestBuilder()
                .setMessageCount(1)
                .createProcessBatch();
    }

    private static String processBatchErrorDescription(CompanionService service) {
        var observer = new CapturingObserver<TRspProcessBatch>();
        service.processBatch(createRequest(), observer);
        return assertErrorDescription(observer);
    }

    private static String assertErrorDescription(CapturingObserver<TRspProcessBatch> observer) {
        assertNull(observer.response);
        assertFalse(observer.completed);
        var statusException = assertInstanceOf(StatusRuntimeException.class, observer.error);
        assertEquals(Status.Code.INTERNAL, statusException.getStatus().getCode());
        String description = statusException.getStatus().getDescription();
        assertNotNull(description);
        return description;
    }

    @Test
    void errorCarriesClassAndMessageIntoStatus() {
        var service = serviceWithFailure(new AssertionError("Got error key 1101"));

        String description = processBatchErrorDescription(service);

        assertTrue(description.contains("java.lang.AssertionError: Got error key 1101"), description);
        assertTrue(description.contains("ComputationId: " + COMPUTATION_ID), description);
    }

    @Test
    void causeChainIsRenderedIntoStatus() {
        var service = serviceWithFailure(
                new RuntimeException("wrapper failed", new IllegalStateException("root cause")));

        String description = processBatchErrorDescription(service);

        assertTrue(description.contains("java.lang.RuntimeException: wrapper failed"), description);
        assertTrue(description.contains("caused by: java.lang.IllegalStateException: root cause"), description);
    }

    @Test
    void nullMessageExceptionProducesUsefulDescription() {
        var service = serviceWithFailure(new RuntimeException());

        String description = processBatchErrorDescription(service);

        assertTrue(description.contains("java.lang.RuntimeException"), description);
        assertFalse(description.contains(": null"), description);
    }

    @Test
    void virtualMachineErrorIsRethrownAfterSurfacingStatus() {
        var service = serviceWithFailure(new OutOfMemoryError("simulated"));
        var observer = new CapturingObserver<TRspProcessBatch>();
        var request = createRequest();

        assertThrows(OutOfMemoryError.class, () -> service.processBatch(request, observer));

        // The status was surfaced before the rethrow.
        String description = assertErrorDescription(observer);
        assertTrue(description.contains("java.lang.OutOfMemoryError: simulated"), description);
    }

    @Test
    void causeDepthIsBounded() {
        RuntimeException chain = new RuntimeException("level 0");
        Throwable current = chain;
        for (int i = 1; i <= 20; i++) {
            Throwable next = new RuntimeException("level " + i);
            current.initCause(next);
            current = next;
        }

        String description = processBatchErrorDescription(serviceWithFailure(chain));

        assertTrue(description.contains("level 10"), description);
        assertFalse(description.contains("level 11"), description);
        assertTrue(description.endsWith("; ..."), description);
    }

    @Test
    void descriptionIsBoundedInUtf8Bytes() {
        // Percent-encoded Cyrillic is up to nine wire bytes per char, so the bound must count
        // UTF-8 bytes; 2048 UTF-16 chars of it would pass a char-based cap and blow the limit.
        String description = processBatchErrorDescription(
                serviceWithFailure(new RuntimeException("\u044f".repeat(4_000))));

        int utf8Bytes = description.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        assertTrue(utf8Bytes <= 2048 + "... (truncated)".length(),
                "Description exceeds the UTF-8 byte bound: " + utf8Bytes);
        assertTrue(description.endsWith("... (truncated)"), description);
    }

    @Test
    void throwingToStringIsSurvived() {
        var evil = new RuntimeException("unreachable") {
            @Override
            public String toString() {
                throw new IllegalStateException("toString exploded");
            }
        };

        String description = processBatchErrorDescription(serviceWithFailure(evil));

        assertTrue(description.contains("toString failed"), description);
    }

    @Test
    void resourceStatusesStayInBandThroughTheInjectedProcessor() {
        var processor = mock(CompanionRequestProcessor.class);
        var service = new CompanionService(processor, new SimpleMeterRegistry());
        var request = TReqResourceExecute.newBuilder()
                .setRequestId(createRequest().getRequestId())
                .setResourceId("r").setCommand(EResourceCommand.RC_INIT).build();
        when(processor.resourceExecute(request)).thenReturn(
                new ExecuteOutcome(EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED, "dependency missing"));
        var observer = new CapturingObserver<TRspResourceExecute>();

        service.resourceExecute(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertEquals(request.getRequestId(), observer.response.getRequestId());
        assertEquals(EResourceExecuteStatus.RES_RESOURCE_NOT_INITIALIZED, observer.response.getStatus());
        assertEquals("dependency missing", observer.response.getError().getMessage());
        verify(processor).resourceExecute(request);
    }

    @Test
    void cancelledBatchAndPutJobDoNotReachProcessor() {
        var processor = mock(CompanionRequestProcessor.class);
        var registry = new SimpleMeterRegistry();
        var service = new CompanionService(processor, registry);
        var batch = createRequest();
        var putJob = TReqPutJob.newBuilder().setRequestId(batch.getRequestId()).setJobId(batch.getJobId())
                .setComputationId(batch.getComputationId()).setJobInfo(batch.getJobInfo()).build();
        var batchObserver = new CapturingObserver<TRspProcessBatch>();
        var jobObserver = new CapturingObserver<TRspPutJob>();
        try (var context = Context.current().withCancellation()) {
            context.cancel(null);
            context.run(() -> {
                service.processBatch(batch, batchObserver);
                service.putJob(putJob, jobObserver);
            });
        }
        verifyNoInteractions(processor);
        assertEquals(Status.Code.CANCELLED, Status.fromThrowable(batchObserver.error).getCode());
        assertEquals(Status.Code.CANCELLED, Status.fromThrowable(jobObserver.error).getCode());
        assertEquals(1, registry.get(CompanionMetrics.REQUEST_DURATION).timer().count());
    }

    @Test
    void failedBatchStopsMeasurementOnce() throws Exception {
        var processor = mock(CompanionRequestProcessor.class);
        var registry = new SimpleMeterRegistry();
        when(processor.processBatch(any())).thenThrow(new AssertionError("batch failed"));
        var observer = new CapturingObserver<TRspProcessBatch>();

        new CompanionService(processor, registry).processBatch(createRequest(), observer);

        assertErrorDescription(observer);
        assertEquals(1, registry.get(CompanionMetrics.REQUEST_DURATION).timer().count());
    }

    @Test
    void nonBatchErrorsUseTheSameBoundedFormatter() {
        var processor = mock(CompanionRequestProcessor.class);
        when(processor.listJobs(any())).thenThrow(new IllegalStateException("я".repeat(4000)));
        var observer = new CapturingObserver<TRspListJobs>();
        new CompanionService(processor, new SimpleMeterRegistry()).listJobs(TReqListJobs.newBuilder()
                .setRequestId(createRequest().getRequestId()).build(), observer);
        var status = Status.fromThrowable(observer.error);
        assertEquals(Status.Code.INTERNAL, status.getCode());
        assertNotNull(status.getDescription());
        assertTrue(status.getDescription().getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 2065);
        assertFalse(observer.completed);
    }

    @Test
    void fatalFailureIsNotMaskedByAnErrorObserver() {
        var fatal = new OutOfMemoryError("simulated");
        var deliveryFailure = new IllegalStateException("observer failed");
        var observer = new CapturingObserver<TRspProcessBatch>() {
            @Override
            public void onError(Throwable error) {
                throw deliveryFailure;
            }
        };
        assertSame(fatal, assertThrows(OutOfMemoryError.class,
                () -> serviceWithFailure(fatal).processBatch(createRequest(), observer)));
        assertSame(deliveryFailure, fatal.getSuppressed()[0]);
    }

    @Test
    void successfulResponseDeliveryFailureDoesNotTriggerAnotherTerminalCallback() {
        var processor = mock(CompanionRequestProcessor.class);
        var request = TReqListJobs.newBuilder().setRequestId(createRequest().getRequestId()).build();
        when(processor.listJobs(request)).thenReturn(java.util.Set.of());
        var deliveryFailure = new IllegalStateException("observer failed");
        var terminalCalls = new AtomicInteger();
        var observer = new CapturingObserver<TRspListJobs>() {
            @Override
            public void onNext(TRspListJobs response) {
                throw deliveryFailure;
            }

            @Override
            public void onError(Throwable error) {
                terminalCalls.incrementAndGet();
            }

            @Override
            public void onCompleted() {
                terminalCalls.incrementAndGet();
            }
        };
        var service = new CompanionService(processor, new SimpleMeterRegistry());
        assertSame(deliveryFailure, assertThrows(IllegalStateException.class,
                () -> service.listJobs(request, observer)));
        assertEquals(0, terminalCalls.get());
    }

    @Test
    void fatalFormatterFailureIsNotSwallowed() {
        var fatal = new OutOfMemoryError("formatter, simulated");
        var failure = new IllegalStateException("body") {
            @Override
            public String toString() {
                throw fatal;
            }
        };
        var observer = new CapturingObserver<TRspProcessBatch>();
        assertSame(fatal, assertThrows(OutOfMemoryError.class,
                () -> serviceWithFailure(failure).processBatch(createRequest(), observer)));
    }

    private static class CapturingObserver<T> implements StreamObserver<T> {
        private T response;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            response = value;
        }

        @Override
        public void onError(Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
