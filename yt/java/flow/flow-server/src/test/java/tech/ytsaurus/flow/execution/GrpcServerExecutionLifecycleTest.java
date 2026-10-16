package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.job.JobContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcServerExecutionLifecycleTest {
    private GrpcServerExecution execution;

    @AfterEach
    void tearDown() {
        if (execution != null) {
            execution.stop();
        }
    }

    @Test
    void concurrentAndIdempotentStartCreatesOneRuntime() throws Exception {
        FakeRuntime runtime = new FakeRuntime(101, 201);
        CountDownLatch starterEntered = new CountDownLatch(1);
        CountDownLatch releaseStarter = new CountDownLatch(1);
        FakeStarter starter = FakeStarter.blocking(runtime, starterEntered, releaseStarter);
        CountingJobContext jobContext = new CountingJobContext();
        AtomicInteger diagnosticsCalls = new AtomicInteger();
        execution = createExecution(starter, jobContext, diagnosticsCalls);

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int index = 0; index < threadCount; index++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    execution.startAsync();
                    return null;
                }));
            }

            start.countDown();
            assertTrue(starterEntered.await(5, TimeUnit.SECONDS));
            releaseStarter.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }

            execution.startAsync();
            assertEquals(1, starter.calls.get());
            assertEquals(1, jobContext.clearCalls.get());
            assertEquals(1, diagnosticsCalls.get());
            assertTrue(execution.isRunning());
            assertEquals(101, execution.getPort());
            assertEquals(201, execution.getMonitoringPort());
        } finally {
            releaseStarter.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void failedStartClosesTheExecution() {
        IOException failure = new IOException("start failed");
        FakeStarter starter = FakeStarter.failing(failure);
        CountingJobContext jobContext = new CountingJobContext();
        execution = createExecution(starter, jobContext, new AtomicInteger());

        assertSame(failure, assertThrows(IOException.class, execution::startAsync));
        assertThrows(IllegalStateException.class, execution::startAsync);
        assertEquals(1, starter.calls.get());
        assertEquals(1, jobContext.clearCalls.get());
        assertFalse(execution.isRunning());
        assertEquals(-1, execution.getPort());
        assertEquals(-1, execution.getMonitoringPort());
    }

    @Test
    void fatalStartClosesTheExecution() {
        OutOfMemoryError failure = new OutOfMemoryError("fatal startup failure");
        FakeStarter starter = new FakeStarter(() -> {
            throw failure;
        });
        execution = createExecution(starter, new CountingJobContext(), new AtomicInteger());

        assertSame(failure, assertThrows(OutOfMemoryError.class, execution::startAsync));
        assertThrows(IllegalStateException.class, execution::startAsync);
        assertEquals(1, starter.calls.get());
    }

    @Test
    void stopClosesBeforeShutdownAndDoesNotHoldTheLock() throws Exception {
        FakeRuntime runtime = new FakeRuntime(103, 203);
        CountDownLatch shutdownEntered = new CountDownLatch(1);
        CountDownLatch releaseShutdown = new CountDownLatch(1);
        runtime.blockShutdown(shutdownEntered, releaseShutdown);
        FakeStarter starter = FakeStarter.returning(runtime);
        execution = createExecution(starter, new CountingJobContext(), new AtomicInteger());
        execution.startAsync();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> stopFuture = executor.submit(execution::stop);
        try {
            assertTrue(shutdownEntered.await(5, TimeUnit.SECONDS));
            Future<?> observer = executor.submit(() -> {
                assertFalse(execution.isRunning());
                assertEquals(-1, execution.getPort());
                assertEquals(-1, execution.getMonitoringPort());
                assertThrows(IllegalStateException.class, execution::startAsync);
                execution.stop();
            });
            observer.get(2, TimeUnit.SECONDS);
        } finally {
            releaseShutdown.countDown();
            stopFuture.get(5, TimeUnit.SECONDS);
            executor.shutdownNow();
        }

        assertEquals(1, runtime.shutdownCalls.get());
        assertThrows(IllegalStateException.class, execution::startAsync);
    }

    @Test
    void shutdownErrorEscapesAfterTheExecutionCloses() throws IOException {
        AssertionError failure = new AssertionError("shutdown failed");
        FakeRuntime runtime = new FakeRuntime(104, 204);
        runtime.shutdownFailure = failure;
        execution = createExecution(FakeStarter.returning(runtime), new CountingJobContext(), new AtomicInteger());
        execution.startAsync();

        assertSame(failure, assertThrows(AssertionError.class, execution::stop));
        assertThrows(IllegalStateException.class, execution::startAsync);
        assertEquals(1, runtime.shutdownCalls.get());
    }

    @Test
    void concurrentStopShutsDownExactlyOnce() throws Exception {
        FakeRuntime runtime = new FakeRuntime(105, 205);
        CountDownLatch shutdownEntered = new CountDownLatch(1);
        CountDownLatch releaseShutdown = new CountDownLatch(1);
        runtime.blockShutdown(shutdownEntered, releaseShutdown);
        execution = createExecution(FakeStarter.returning(runtime), new CountingJobContext(), new AtomicInteger());
        execution.startAsync();

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int index = 0; index < threadCount; index++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    execution.stop();
                    return null;
                }));
            }
            start.countDown();
            assertTrue(shutdownEntered.await(5, TimeUnit.SECONDS));
            releaseShutdown.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            releaseShutdown.countDown();
            executor.shutdownNow();
        }

        assertEquals(1, runtime.shutdownCalls.get());
        assertFalse(execution.isRunning());
    }

    @Test
    void interruptedBlockingStartStopsTheRuntime() throws Exception {
        FakeRuntime runtime = new FakeRuntime(106, 206);
        execution = createExecution(FakeStarter.returning(runtime), new CountingJobContext(), new AtomicInteger());
        AtomicReference<Throwable> startFailure = new AtomicReference<>();
        Thread blockingStart = new Thread(() -> {
            try {
                execution.start();
            } catch (Throwable e) {
                startFailure.set(e);
            }
        }, "fake-runtime-blocking-start");
        blockingStart.start();

        assertTrue(runtime.awaitEntered.await(5, TimeUnit.SECONDS));
        blockingStart.interrupt();
        blockingStart.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(blockingStart.isAlive());
        assertInstanceOf(InterruptedException.class, startFailure.get());
        assertTrue(blockingStart.isInterrupted());
        assertEquals(1, runtime.shutdownCalls.get());
        assertThrows(IllegalStateException.class, execution::startAsync);
    }

    private static GrpcServerExecution createExecution(
            FakeStarter starter,
            CountingJobContext jobContext,
            AtomicInteger diagnosticsCalls
    ) {
        return new GrpcServerExecution(jobContext, starter, diagnosticsCalls::incrementAndGet);
    }

    @FunctionalInterface
    private interface StarterResult {
        CompanionServerRuntime start() throws IOException;
    }

    private static final class FakeStarter implements CompanionServerRuntime.Starter {
        private final StarterResult result;
        private final AtomicInteger calls = new AtomicInteger();

        private FakeStarter(StarterResult result) {
            this.result = result;
        }

        static FakeStarter returning(FakeRuntime runtime) {
            return new FakeStarter(() -> runtime);
        }

        static FakeStarter failing(IOException failure) {
            return new FakeStarter(() -> {
                throw failure;
            });
        }

        static FakeStarter blocking(
                FakeRuntime runtime,
                CountDownLatch entered,
                CountDownLatch release
        ) {
            return new FakeStarter(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("start interrupted", e);
                }
                return runtime;
            });
        }

        @Override
        public CompanionServerRuntime start() throws IOException {
            calls.incrementAndGet();
            return result.start();
        }
    }

    private static final class FakeRuntime implements CompanionServerRuntime {
        private final int grpcPort;
        private final int monitoringPort;
        private volatile boolean serving = true;
        private final AtomicInteger shutdownCalls = new AtomicInteger();
        private final CountDownLatch awaitEntered = new CountDownLatch(1);
        private final CountDownLatch terminated = new CountDownLatch(1);
        private volatile Throwable shutdownFailure;
        private volatile CountDownLatch shutdownEntered;
        private volatile CountDownLatch releaseShutdown;

        private FakeRuntime(int grpcPort, int monitoringPort) {
            this.grpcPort = grpcPort;
            this.monitoringPort = monitoringPort;
        }

        void blockShutdown(CountDownLatch entered, CountDownLatch release) {
            shutdownEntered = entered;
            releaseShutdown = release;
        }

        @Override
        public boolean isServing() {
            return serving;
        }

        @Override
        public int grpcPort() {
            return grpcPort;
        }

        @Override
        public int monitoringPort() {
            return monitoringPort;
        }

        @Override
        public void awaitTermination() throws InterruptedException {
            awaitEntered.countDown();
            terminated.await();
        }

        @Override
        public void shutdown() {
            shutdownCalls.incrementAndGet();
            serving = false;
            CountDownLatch entered = shutdownEntered;
            if (entered != null) {
                entered.countDown();
            }
            CountDownLatch release = releaseShutdown;
            if (release != null) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            terminated.countDown();
            if (shutdownFailure != null) {
                throwUnchecked(shutdownFailure);
            }
        }
    }

    private static final class CountingJobContext extends JobContext {
        private final AtomicInteger clearCalls = new AtomicInteger();

        @Override
        public void clear() {
            clearCalls.incrementAndGet();
            super.clear();
        }
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException(failure);
    }
}
