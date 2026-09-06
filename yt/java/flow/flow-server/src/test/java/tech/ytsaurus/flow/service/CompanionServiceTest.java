package tech.ytsaurus.flow.service;

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
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TRspProcessBatch;
import tech.ytsaurus.flow.testutils.ProtobufRequestBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        var observer = new CapturingObserver();
        service.processBatch(createRequest(), observer);
        return assertErrorDescription(observer);
    }

    private static String assertErrorDescription(CapturingObserver observer) {
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
        var observer = new CapturingObserver();
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

    private static final class CapturingObserver implements StreamObserver<TRspProcessBatch> {
        private TRspProcessBatch response;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(TRspProcessBatch value) {
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
