package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.MetricsContextSnapshot;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.job.JobContext;

/**
 * Companion execution entry point.
 * <p>
 * Starts GRPC server and serve requests from worker.
 * <p>
 * This class supports the two application entry points used by the Java companion:
 * <ul>
 *     <li><b>Blocking mode</b>: {@link #start()} is used by non-Spring {@code FlowApplication}.</li>
 *     <li><b>Spring mode</b>: {@link #startAsync()} and {@link #stop()} are called by
 *     {@code FlowCompanionLifecycle} as the application context starts and closes.</li>
 * </ul>
 * <p>Instances are created from a {@link CompanionExecutionSpec}. All configuration — pipeline
 * context, config, job context and HTTP handlers — is copied at construction and is immutable afterward.
 * A start attempt is one-shot: repeated calls while running are idempotent, while startup failure or
 * shutdown closes the execution permanently. Calling {@link #stop()} before start remains a no-op.
 * The worker restarts a companion by creating a new JVM process.
 *
 * <p>NOTE: this class also owns the monitoring HTTP server in addition to the GRPC server.
 */
public final class GrpcServerExecution implements CompanionExecution {
    private static final Logger log = LoggerFactory.getLogger(GrpcServerExecution.class);

    private final JobContext jobContext;
    private final CompanionServerRuntime.Starter runtimeStarter;
    private final Runnable startupDiagnostics;
    private final ReentrantLock lock = new ReentrantLock();

    // Guarded by lock: runtime is non-null exactly while state is RUNNING.
    private LifecycleState state = LifecycleState.NEW;
    private @Nullable CompanionServerRuntime runtime;
    private @Nullable Thread shutdownHook;

    /**
     * Builds the execution from a {@link CompanionExecutionSpec}.
     * The pipeline context is snapshotted and the handlers are copied here, freezing the configuration phase.
     * When the spec carries no config or job context, it defaults from the environment.
     *
     * @param spec the configuration holder, typically created via
     *             {@code new CompanionExecutionSpec(context)} and the {@code set/add} methods.
     */
    public GrpcServerExecution(CompanionExecutionSpec spec) {
        CompanionExecutionConfig config = spec.getConfig() != null
                ? spec.getConfig()
                : CompanionExecutionConfig.fromEnvironment();
        JobContext resolvedJobContext = spec.getJobContext() != null
                ? spec.getJobContext()
                : new JobContext();
        this.jobContext = resolvedJobContext;
        this.runtimeStarter = new GrpcCompanionServerStarter(
                config,
                new PipelineContextSnapshot(spec.getPipelineContext()),
                resolvedJobContext,
                spec.getPipelineContext().getMetricsContext(),
                spec.getHttpHandlers()
        );
        this.startupDiagnostics = JvmDiagnostics::warnIfHeapDumpOptionsNotSet;
    }

    GrpcServerExecution(
            JobContext jobContext,
            CompanionServerRuntime.Starter runtimeStarter,
            Runnable startupDiagnostics
    ) {
        this.jobContext = Objects.requireNonNull(jobContext, "jobContext");
        this.runtimeStarter = Objects.requireNonNull(runtimeStarter, "runtimeStarter");
        this.startupDiagnostics = Objects.requireNonNull(startupDiagnostics, "startupDiagnostics");
    }

    /**
     * Start GRPC server in blocking mode.
     * <p>
     * This method starts the server and blocks until it is terminated.
     * A shutdown hook is registered to gracefully stop the server on JVM shutdown.
     * <p>
     * If the calling thread is interrupted while waiting, the server is stopped gracefully,
     * the interrupt flag is restored, and {@link InterruptedException} is re-thrown.
     *
     * @throws InterruptedException In case of thread interruption.
     * @throws IOException          In case of I/O error.
     */
    @Override
    public void start() throws InterruptedException, IOException {
        registerShutdownHook();
        CompanionServerRuntime startedRuntime = startOrGetRuntime();
        try {
            startedRuntime.awaitTermination();
        } catch (InterruptedException e) {
            log.info("Interrupted while waiting for companion server, stopping it");
            try {
                stop();
            } catch (RuntimeException | Error stopFailure) {
                e.addSuppressed(stopFailure);
            }
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /**
     * Start GRPC server asynchronously for the Spring application lifecycle.
     *
     * @throws IOException In case of I/O error during server startup.
     */
    @Override
    public void startAsync() throws IOException {
        startOrGetRuntime();
    }

    private CompanionServerRuntime startOrGetRuntime() throws IOException {
        lock.lock();
        try {
            if (state == LifecycleState.RUNNING) {
                return Objects.requireNonNull(runtime);
            }
            if (state == LifecycleState.STARTING) {
                throw new IllegalStateException("Companion server is already starting");
            }
            if (state == LifecycleState.CLOSED) {
                throw new IllegalStateException("Companion execution is already closed");
            }

            state = LifecycleState.STARTING;
            try {
                startupDiagnostics.run();
                jobContext.clear();
                CompanionServerRuntime startedRuntime = Objects.requireNonNull(runtimeStarter.start());
                runtime = startedRuntime;
                state = LifecycleState.RUNNING;
                return startedRuntime;
            } catch (IOException | RuntimeException | Error e) {
                state = LifecycleState.CLOSED;
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Shut down the in-process companion server runtime.
     * <p>
     * This path supports local and Spring lifecycle cleanup. Worker-managed process termination may bypass it.
     * The underlying {@link io.micrometer.core.instrument.MeterRegistry} remains owned by the caller;
     * only the runtime-scoped {@link MetricsContextSnapshot} is closed.
     */
    @Override
    public void stop() {
        CompanionServerRuntime runtimeToShutdown;
        lock.lock();
        try {
            if (state != LifecycleState.RUNNING) {
                return;
            }
            runtimeToShutdown = Objects.requireNonNull(runtime);
            // Close admission before the slow shutdown so concurrent stop calls remain idempotent.
            runtime = null;
            state = LifecycleState.CLOSED;
        } finally {
            lock.unlock();
        }
        runtimeToShutdown.shutdown();
    }

    /**
     * Returns whether the server is currently running.
     *
     * @return true if the server is running, false otherwise.
     */
    @Override
    public boolean isRunning() {
        lock.lock();
        try {
            return state == LifecycleState.RUNNING && Objects.requireNonNull(runtime).isServing();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the port on which the gRPC server is listening.
     *
     * @return The server port, or -1 if the server is not running.
     */
    @Override
    public int getPort() {
        lock.lock();
        try {
            if (state == LifecycleState.RUNNING && Objects.requireNonNull(runtime).isServing()) {
                return runtime.grpcPort();
            }
            return -1;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the port the monitoring HTTP server is listening on, or {@code -1} if it is not running.
     */
    @Override
    public int getMonitoringPort() {
        lock.lock();
        try {
            return state == LifecycleState.RUNNING ? Objects.requireNonNull(runtime).monitoringPort() : -1;
        } finally {
            lock.unlock();
        }
    }

    private void registerShutdownHook() {
        lock.lock();
        try {
            if (shutdownHook == null) {
                Thread hook = new Thread(() -> {
                    log.info("Shutdown hook triggered, stopping gRPC server");
                    try {
                        stop();
                    } catch (RuntimeException | Error e) {
                        log.error("Failed to stop companion server in shutdown hook", e);
                        throw e;
                    }
                }, "grpc-shutdown-hook");
                Runtime.getRuntime().addShutdownHook(hook);
                shutdownHook = hook;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * One-shot lifecycle: NEW -> STARTING -> RUNNING -> CLOSED, or STARTING -> CLOSED on failure.
     * CLOSED refuses new starts even while physical shutdown is still in progress. STARTING is visible
     * only to reentrant callbacks; ordinary concurrent callers wait for the lifecycle lock.
     */
    private enum LifecycleState {
        NEW,
        STARTING,
        RUNNING,
        CLOSED,
    }
}
