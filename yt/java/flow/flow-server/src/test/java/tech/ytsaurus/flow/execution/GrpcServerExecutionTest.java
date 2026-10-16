package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.TGuid;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.MetricsContext;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.ResourceContext;
import tech.ytsaurus.flow.rpc.CompanionServiceGrpc;
import tech.ytsaurus.flow.rpc.EResourceCommand;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;
import tech.ytsaurus.flow.rpc.EResponseStatus;
import tech.ytsaurus.flow.rpc.TReqCompanionInfo;
import tech.ytsaurus.flow.rpc.TReqResourceExecute;
import tech.ytsaurus.flow.testutils.ComputationTestUtils;
import tech.ytsaurus.flow.testutils.ProtobufRequestBuilder;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTree;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link GrpcServerExecution}.
 */
class GrpcServerExecutionTest {

    private GrpcServerExecution execution;

    @BeforeEach
    void setUp() {
        UnloadCountingResource.UNLOAD_COUNT.set(0);
        execution = createExecution();
    }

    @AfterEach
    void tearDown() {
        if (execution != null) {
            execution.stop();
        }
    }

    private static CompanionExecutionConfig buildConfig(int port) {
        return CompanionExecutionConfig.builder()
                .port(port)
                .clusterUrl("test-cluster")
                .pipelinePath("//test/pipeline")
                .build();
    }

    private GrpcServerExecution createExecution() {
        return new GrpcServerExecution(new CompanionExecutionSpec(new PipelineContext()).setConfig(buildConfig(0)));
    }

    private GrpcServerExecution createExecution(int port) {
        return new GrpcServerExecution(new CompanionExecutionSpec(new PipelineContext()).setConfig(buildConfig(port)));
    }

    private HealthCheckResponse checkHealth(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        try {
            return HealthGrpc.newBlockingStub(channel)
                    .check(HealthCheckRequest.newBuilder().setService("").build());
        } finally {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean companionInfoContainsComputation(int port, String computationId) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        try {
            var response = CompanionServiceGrpc.newBlockingStub(channel)
                    .companionInfo(TReqCompanionInfo.getDefaultInstance());
            return YsonUtils.yTreeFromProto(response.getPayload())
                    .mapNode()
                    .getOrThrow("computations")
                    .mapNode()
                    .containsKey(computationId);
        } finally {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Initializes one {@link UnloadCountingResource} instance in the companion serving on the port.
     */
    private void initResource(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        try {
            var argument = YsonUtils.protoFromYTree(YTree.builder().beginMap()
                    .key("spec").beginMap()
                    .key("resource_class_name").value("WorkerProxy")
                    .key("parameters").beginMap()
                    .key("companion_resource_class").value("UnloadCountingResource")
                    .endMap()
                    .endMap()
                    .key("dynamic_spec").beginMap()
                    .key("parameters").beginMap().endMap()
                    .endMap()
                    .key("incarnation_id").value("1-2-3-4")
                    .key("incarnation_generation").value(1)
                    .key("configuration_generation").value(0)
                    .key("dependencies").beginList().endList()
                    .endMap().build());
            var response = CompanionServiceGrpc.newBlockingStub(channel)
                    .resourceExecute(TReqResourceExecute.newBuilder()
                            .setRequestId(TGuid.newBuilder().setFirst(1).setSecond(2).build())
                            .setResourceId("r")
                            .setCommand(EResourceCommand.RC_INIT)
                            .setArgument(argument)
                            .build());
            assertEquals(EResourceExecuteStatus.RES_OK, response.getStatus());
        } finally {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void serverLifecycle() throws IOException {
        // Before start.
        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort());
        assertEquals(-1, execution.getMonitoringPort());

        // After start.
        execution.startAsync();
        assertTrue(execution.isRunning());
        int port = execution.getPort();
        int monitoringPort = execution.getMonitoringPort();
        assertTrue(port > 0, "Port should be assigned after start");
        assertTrue(monitoringPort > 0, "Monitoring port should be assigned after start");

        // Port remains consistent while running.
        assertEquals(port, execution.getPort());
        assertEquals(monitoringPort, execution.getMonitoringPort());

        // After stop.
        execution.stop();
        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort(), "Port should be -1 after stop");
        assertEquals(-1, execution.getMonitoringPort(), "Monitoring port should be -1 after stop");
    }

    @Test
    void startAsyncIsIdempotent() throws IOException {
        execution.startAsync();
        int port = execution.getPort();

        execution.startAsync();
        assertEquals(port, execution.getPort(), "Port should not change on second startAsync");
    }

    @Test
    void stopIsIdempotent() throws IOException {
        execution.startAsync();
        execution.stop();
        execution.stop();

        assertFalse(execution.isRunning());
    }

    @Test
    void stopWithoutStartIsNoOp() {
        execution.stop();

        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort());
    }

    @Test
    void healthServiceReturnsServingWhenRunning() throws IOException {
        execution.startAsync();

        HealthCheckResponse response = checkHealth(execution.getPort());

        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    }

    @Test
    void contextIsSnapshottedAtConstruction() throws IOException {
        var context = new PipelineContext();

        context.registerComputation(ComputationTestUtils.passthroughComputation("before_construction"));
        var localExecution =
                new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(buildConfig(0)));
        execution = localExecution;
        context.registerComputation(ComputationTestUtils.passthroughComputation("after_construction"));

        localExecution.startAsync();

        var port = localExecution.getPort();
        assertTrue(companionInfoContainsComputation(port, "before_construction"));
        assertFalse(companionInfoContainsComputation(port, "after_construction"));
    }

    @Test
    void processBatchPublishesCompanionMetrics() throws IOException {
        var registry = new SimpleMeterRegistry();
        var context = new PipelineContext();
        context.registerComputation(ComputationTestUtils.passthroughComputation(
                ProtobufRequestBuilder.COMPUTATION_ID
        ));
        context.registerMetricsContext(MetricsContext.builder()
                .withMeterRegistry(registry)
                .withJvmMetrics(false)
                .build());
        execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(buildConfig(0)));
        execution.startAsync();

        var request = new ProtobufRequestBuilder()
                .setMessageCount(2)
                .addInternalState("test-state", 100)
                .createProcessBatch();
        var followUpRequest = new ProtobufRequestBuilder()
                .setMessageCount(1)
                .setAddJobInfo(false)
                .createProcessBatch();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", execution.getPort())
                .usePlaintext()
                .build();
        try {
            var response = CompanionServiceGrpc.newBlockingStub(channel).processBatch(request);
            assertEquals(EResponseStatus.RS_OK, response.getStatus());

            var followUpResponse = CompanionServiceGrpc.newBlockingStub(channel).processBatch(followUpRequest);
            assertEquals(EResponseStatus.RS_OK, followUpResponse.getStatus());
        } finally {
            channel.shutdown();
        }

        var requestCount = registry.get("yt.flow.companion.request.count")
                .tag("request_type", "process_batch")
                .tag("computation_id", ProtobufRequestBuilder.COMPUTATION_ID)
                .counter();
        assertEquals(2.0, requestCount.count());

        var requestSize = registry.get("yt.flow.companion.request.size")
                .tag("request_type", "process_batch")
                .tag("computation_id", ProtobufRequestBuilder.COMPUTATION_ID)
                .summary();
        assertEquals(2, requestSize.count());
        assertEquals(request.getSerializedSize() + followUpRequest.getSerializedSize(), requestSize.totalAmount());

        var requestDuration = registry.get("yt.flow.companion.request.duration")
                .tag("request_type", "process_batch")
                .tag("computation_id", ProtobufRequestBuilder.COMPUTATION_ID)
                .timer();
        assertEquals(2, requestDuration.count());

        var stateSize = registry.get("yt.flow.companion.state.size")
                .tag("request_type", "process_batch")
                .tag("computation_id", ProtobufRequestBuilder.COMPUTATION_ID)
                .tag("direction", "request")
                .tag("state_type", "internal")
                .tag("state_name", "test-state")
                .summary();
        assertEquals(1, stateSize.count());
        assertEquals(request.getInternalStates(0).getSerializedSize(), stateSize.totalAmount());

        var jobRecreationCount = registry.get("yt.flow.companion.job.recreation.count")
                .tag("request_type", "process_batch")
                .tag("computation_id", ProtobufRequestBuilder.COMPUTATION_ID)
                .counter();
        assertEquals(1.0, jobRecreationCount.count());
    }

    @Test
    void resourceExecuteRoundTrip() throws IOException {
        execution.startAsync();

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", execution.getPort())
                .usePlaintext()
                .build();
        try {
            var stub = CompanionServiceGrpc.newBlockingStub(channel);
            var requestId = TGuid.newBuilder().setFirst(1).setSecond(2).build();

            var initArgument = YsonUtils.protoFromYTree(YTree.builder().beginMap()
                    .key("spec").beginMap()
                    .key("resource_class_name").value("WorkerProxy")
                    .key("parameters").beginMap()
                    .key("companion_resource_class").value("UnknownResource")
                    .endMap()
                    .endMap()
                    .key("dynamic_spec").beginMap()
                    .key("parameters").beginMap().endMap()
                    .endMap()
                    .key("incarnation_id").value("1-2-3-4")
                    .key("incarnation_generation").value(1)
                    .key("configuration_generation").value(0)
                    .key("dependencies").beginList().endList()
                    .endMap().build());
            var notFoundResponse = stub.resourceExecute(TReqResourceExecute.newBuilder()
                    .setRequestId(requestId)
                    .setResourceId("r")
                    .setCommand(EResourceCommand.RC_INIT)
                    .setArgument(initArgument)
                    .build());
            assertEquals(EResourceExecuteStatus.RES_RESOURCE_NOT_FOUND, notFoundResponse.getStatus());
            assertTrue(notFoundResponse.hasError());
            assertTrue(notFoundResponse.getError().getMessage().contains("UnknownResource"));

            var malformedResponse = stub.resourceExecute(TReqResourceExecute.newBuilder()
                    .setRequestId(requestId)
                    .setResourceId("r")
                    .setCommand(EResourceCommand.RC_INIT)
                    .build());
            assertEquals(EResourceExecuteStatus.RES_ERROR, malformedResponse.getStatus());
            assertTrue(malformedResponse.hasError());
        } finally {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void independentExecutionsUseDifferentPorts() throws IOException {
        GrpcServerExecution execution2 = createExecution();

        try {
            execution.startAsync();
            execution2.startAsync();

            assertTrue(execution.isRunning());
            assertTrue(execution2.isRunning());
            assertNotEquals(execution.getPort(), execution2.getPort());
        } finally {
            execution2.stop();
        }
    }

    @Test
    void newExecutionCanStartAfterPreviousOneStops() throws IOException {
        for (int i = 0; i < 10; i++) {
            // Stop previous execution before creating a new one to avoid leaking server instances.
            if (execution != null && execution.isRunning()) {
                execution.stop();
            }
            execution = createExecution();
            execution.startAsync();
            assertTrue(execution.isRunning());
            assertTrue(execution.getPort() > 0);
            execution.stop();
            assertFalse(execution.isRunning());
        }
    }

    @Test
    void startBlocksUntilStop() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        Future<?> start = executor.submit(() -> {
            execution.start();
            return null;
        });
        try {
            await()
                    .atMost(5, TimeUnit.SECONDS)
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .until(execution::isRunning);

            assertThrows(TimeoutException.class, () -> start.get(500, TimeUnit.MILLISECONDS));
            execution.stop();
            start.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void stopUnloadsHostedResourcesAndClosesExecution() throws IOException {
        var context = new PipelineContext();
        context.registerResourceClass("UnloadCountingResource", UnloadCountingResource::new);
        execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(buildConfig(0)));

        execution.startAsync();
        initResource(execution.getPort());
        assertEquals(0, UnloadCountingResource.UNLOAD_COUNT.get());

        execution.stop();

        assertFalse(execution.isRunning());
        assertEquals(1, UnloadCountingResource.UNLOAD_COUNT.get());
        assertThrows(IllegalStateException.class, execution::startAsync);
    }

    @Test
    void stopWaitsForResourceUnloadAndKeepsExecutionClosed() throws Exception {
        var unloadEntered = new CountDownLatch(1);
        var unloadMayFinish = new CountDownLatch(1);
        var context = new PipelineContext();
        context.registerResourceClass(
                "UnloadCountingResource", () -> new BlockingUnloadResource(unloadEntered, unloadMayFinish));
        execution = new GrpcServerExecution(new CompanionExecutionSpec(context).setConfig(buildConfig(0)));
        execution.startAsync();
        initResource(execution.getPort());

        var stopReturned = new CountDownLatch(1);
        var stopThread = new Thread(() -> {
            try {
                execution.stop();
            } finally {
                stopReturned.countDown();
            }
        }, "test-resource-stop");
        stopThread.start();
        try {
            assertTrue(unloadEntered.await(5, TimeUnit.SECONDS));
            assertFalse(stopReturned.await(200, TimeUnit.MILLISECONDS));
            assertFalse(execution.isRunning());
            assertThrows(IllegalStateException.class, execution::startAsync);
        } finally {
            unloadMayFinish.countDown();
        }

        assertTrue(stopReturned.await(5, TimeUnit.SECONDS));
        stopThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(stopThread.isAlive());
        assertEquals(1, UnloadCountingResource.UNLOAD_COUNT.get());
        assertThrows(IllegalStateException.class, execution::startAsync);
    }

    @Test
    void restartAfterStopIsRejected() throws IOException {
        execution.startAsync();
        execution.stop();

        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort());
        assertThrows(IllegalStateException.class, execution::startAsync);
    }

    @Test
    void serverUnreachableAfterStop() throws IOException {
        execution.startAsync();
        int port = execution.getPort();

        // Verify reachable.
        assertEquals(HealthCheckResponse.ServingStatus.SERVING, checkHealth(port).getStatus());

        execution.stop();

        // Verify unreachable — connection should fail.
        assertThrows(StatusRuntimeException.class, () -> checkHealth(port));
    }

    @Test
    void startupFailureRequiresANewExecution() throws IOException {
        execution.startAsync();
        int port = execution.getPort();
        var failedExecution = createExecution(port);
        try {
            assertThrows(IOException.class, failedExecution::startAsync);
            assertFalse(failedExecution.isRunning());
            assertEquals(-1, failedExecution.getPort());

            execution.stop();
            assertThrows(IllegalStateException.class, failedExecution::startAsync);

            var replacementExecution = createExecution(port);
            try {
                replacementExecution.startAsync();
                assertEquals(HealthCheckResponse.ServingStatus.SERVING, checkHealth(port).getStatus());
            } finally {
                replacementExecution.stop();
            }
        } finally {
            failedExecution.stop();
        }
    }

    @Test
    void getPortReturnsActualPortNotZero() throws IOException {
        // Config uses port 0 (auto-assign).
        execution.startAsync();
        int port = execution.getPort();

        assertNotEquals(0, port, "getPort() should return actual assigned port, not 0");
        assertTrue(port > 0, "Port should be a positive number");
    }

    @Test
    void concurrentStartAsyncOnlyStartsOnce() throws Exception {
        int numThreads = 10;
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(numThreads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < numThreads; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    execution.startAsync();
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }

            // All calls completed successfully, but only one server should exist.
            assertTrue(execution.isRunning());
            assertTrue(execution.getPort() > 0);

            // Verify the server is actually functional.
            HealthCheckResponse response = checkHealth(execution.getPort());
            assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
        } finally {
            executor.shutdownNow();
        }
    }

    static final class UnloadCountingResource implements FlowResource {
        static final AtomicInteger UNLOAD_COUNT = new AtomicInteger();

        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() {
            UNLOAD_COUNT.incrementAndGet();
        }
    }

    static final class BlockingUnloadResource implements FlowResource {
        private final CountDownLatch unloadEntered;
        private final CountDownLatch unloadMayFinish;

        BlockingUnloadResource(CountDownLatch unloadEntered, CountDownLatch unloadMayFinish) {
            this.unloadEntered = unloadEntered;
            this.unloadMayFinish = unloadMayFinish;
        }

        @Override
        public void load(ResourceContext context) {
        }

        @Override
        public void unload() throws Exception {
            unloadEntered.countDown();
            assertTrue(unloadMayFinish.await(5, TimeUnit.SECONDS));
            UnloadCountingResource.UNLOAD_COUNT.incrementAndGet();
        }
    }
}
