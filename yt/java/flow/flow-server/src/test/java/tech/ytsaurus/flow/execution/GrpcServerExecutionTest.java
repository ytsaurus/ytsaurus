package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.rpc.CompanionServiceGrpc;
import tech.ytsaurus.flow.rpc.TReqCompanionInfo;
import tech.ytsaurus.flow.testutils.ComputationTestUtils;
import tech.ytsaurus.flow.utils.YsonUtils;

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
        execution = createExecution();
    }

    @AfterEach
    void tearDown() {
        if (execution != null && execution.isRunning()) {
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

    @Test
    void serverLifecycle() throws IOException {
        // Before start.
        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort());

        // After start.
        execution.startAsync();
        assertTrue(execution.isRunning());
        int port = execution.getPort();
        assertTrue(port > 0, "Port should be assigned after start");

        // Port remains consistent while running.
        assertEquals(port, execution.getPort());

        // After stop.
        execution.stop();
        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort(), "Port should be -1 after stop");
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
    void concurrentStartStopOperations() throws InterruptedException {
        int numThreads = 10;
        AtomicInteger errorCount = new AtomicInteger(0);
        try (var executor = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);

            for (int i = 0; i < numThreads; i++) {
                final boolean shouldStart = i % 2 == 0;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        if (shouldStart) {
                            execution.startAsync();
                        } else {
                            execution.stop();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (IOException e) {
                        // startAsync may throw IOException — this is unexpected in this test.
                        errorCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All operations should complete");
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

            assertEquals(0, errorCount.get(), "No unexpected errors should occur");

            // Server should be in a consistent state:
            // running = true & port > 0 || running = false & port = -1
            boolean isRunning = execution.isRunning();
            int port = execution.getPort();
            if (isRunning) {
                assertTrue(port > 0, "Running server should have a positive port");
                // Verify the server is actually reachable.
                assertEquals(HealthCheckResponse.ServingStatus.SERVING, checkHealth(port).getStatus());
            } else {
                assertEquals(-1, port, "Stopped server should report port -1");
            }
        }
    }

    @Test
    void multipleServerInstances() throws IOException {
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
    void rapidStartStopCycles() throws IOException {
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
        CountDownLatch startReturned = new CountDownLatch(1);

        Thread serverThread = new Thread(() -> {
            try {
                execution.start();
            } catch (Exception e) {
                // Expected — start() may throw InterruptedException when stop() terminates the server.
            } finally {
                startReturned.countDown();
            }
        }, "test-start-thread");
        serverThread.start();

        // Wait for server to be running.
        await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .until(() -> execution.isRunning());

        assertTrue(execution.isRunning(), "Server should be running");

        // start() should still be blocking.
        assertFalse(startReturned.await(500, TimeUnit.MILLISECONDS),
                "start() should still be blocking");

        // Stop should unblock start().
        execution.stop();
        assertTrue(startReturned.await(5, TimeUnit.SECONDS),
                "start() should return after stop()");

        serverThread.join(5000);
        assertFalse(serverThread.isAlive(), "Server thread should have terminated");
    }

    @Test
    void restartAfterStop() throws IOException {
        execution.startAsync();
        int firstPort = execution.getPort();
        assertTrue(firstPort > 0);

        execution.stop();
        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort());

        execution.startAsync();
        assertTrue(execution.isRunning());
        int secondPort = execution.getPort();
        assertTrue(secondPort > 0);

        // Verify server is actually functional after restart.
        HealthCheckResponse response = checkHealth(secondPort);
        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
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
    void startAsyncFailureKeepsServerStopped() throws IOException {
        execution.startAsync();
        int port = execution.getPort();

        // Try to start another server on the same port — should fail.
        var failingExecution = createExecution(port);
        assertThrows(IOException.class, failingExecution::startAsync);

        // Verify the failing execution is in a clean state.
        assertFalse(failingExecution.isRunning());
        assertEquals(-1, failingExecution.getPort());

        // Verify stop on failed execution is safe.
        failingExecution.stop();
        assertFalse(failingExecution.isRunning());
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
        CountDownLatch done = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    start.await();
                    execution.startAsync();
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    // Unexpected.
                } finally {
                    done.countDown();
                }
            }).start();
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "All threads should complete");

        // All threads should succeed (idempotent), but only one server should exist.
        assertEquals(numThreads, successCount.get(), "All startAsync calls should succeed");
        assertTrue(execution.isRunning());
        assertTrue(execution.getPort() > 0);

        // Verify the server is actually functional.
        HealthCheckResponse response = checkHealth(execution.getPort());
        assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
    }
}
