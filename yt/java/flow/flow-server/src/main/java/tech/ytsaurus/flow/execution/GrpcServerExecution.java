package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.net.httpserver.HttpHandler;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.MetricsContext;
import tech.ytsaurus.flow.context.MetricsContextSnapshot;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.service.CompanionService;

/**
 * Companion execution entry point.
 * <p>
 * Starts GRPC server and serve requests from worker.
 * <p>
 * This class supports two modes of operation:
 * <ul>
 *     <li><b>Blocking mode</b>: Use {@link #start()} to start the server and block until termination.
 *     This is suitable for standalone applications.</li>
 *     <li><b>Async mode</b>: Use {@link #startAsync()} to start the server without blocking,
 *     and {@link #stop()} to stop it. This is suitable for integration with lifecycle managers
 *     like Spring's SmartLifecycle.</li>
 * </ul>
 * <p>Instances are created from a {@link CompanionExecutionSpec}. All configuration — pipeline
 * context, config, job context and HTTP handlers — is copied at construction and is immutable afterward.
 *
 * <p>NOTE: this class also owns the monitoring HTTP server in addition to the GRPC server.
 */
public final class GrpcServerExecution implements CompanionExecution {
    private static final Logger log = LoggerFactory.getLogger(GrpcServerExecution.class);

    /**
     * Default timeout in seconds for graceful server shutdown.
     */
    private static final int DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;
    private static final int AWAIT_TIMEOUT_SECONDS = 10;

    private final CompanionExecutionConfig config;
    private final JobContext jobContext;
    private final PipelineContextSnapshot pipelineContextSnapshot;
    private final MetricsContext metricsContext;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, HttpHandler> httpHandlers;

    private @Nullable MonitoringHttpServer monitoringServer;
    private @Nullable Server server;
    private @Nullable HealthStatusManager healthManager;
    private @Nullable MetricsContextSnapshot metricsSnapshot;
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
        CompanionExecutionConfig resolvedConfig = spec.getConfig() != null
                ? spec.getConfig()
                : CompanionExecutionConfig.fromEnvironment();
        this.config = resolvedConfig;
        this.jobContext = spec.getJobContext() != null
                ? spec.getJobContext()
                : new JobContext(resolvedConfig.jobTtl());
        this.pipelineContextSnapshot = new PipelineContextSnapshot(spec.getPipelineContext());
        this.metricsContext = spec.getPipelineContext().getMetricsContext();
        this.httpHandlers = spec.getHttpHandlers();
    }

    /**
     * Start GRPC server in blocking mode.
     * <p>
     * This method starts the server and blocks until it is terminated.
     * A shutdown hook is registered to gracefully stop the server on JVM shutdown.
     * <p>
     * If the calling thread is interrupted while waiting, the server is stopped gracefully,
     * the interrupt flag is restored, and {@link InterruptedException} is re-thrown.
     * <p>
     * This method is suitable for standalone applications.
     *
     * @throws InterruptedException In case of thread interruption.
     * @throws IOException          In case of I/O error.
     */
    @Override
    public void start() throws InterruptedException, IOException {
        // Register shutdown hook before starting the server to avoid a race
        // where the JVM begins shutting down after startAsync() but before
        // the hook is registered.
        // The hook is registered at most once, even if start() is called multiple times.
        lock.lock();
        try {
            if (shutdownHook == null) {
                this.shutdownHook = new Thread(() -> {
                    log.info("Shutdown hook triggered, stopping gRPC server");
                    stop();
                }, "grpc-shutdown-hook");
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            }
        } finally {
            lock.unlock();
        }

        Server startedServer = startServerInternal();

        // Block until termination.
        if (startedServer != null) {
            try {
                startedServer.awaitTermination();
            } catch (InterruptedException e) {
                log.info("Interrupted while waiting for gRPC server termination, stopping server");
                stop();
                Thread.currentThread().interrupt();
                throw e;
            }
        }
    }

    /**
     * Start GRPC server asynchronously without blocking.
     * <p>
     * This method starts the server and returns immediately.
     * Use {@link #stop()} to stop the server.
     * <p>
     * <b>Note:</b> This method does not create any non-daemon threads and does not
     * prevent the JVM from exiting. The gRPC server uses daemon threads internally
     * (Netty event loop), so the JVM will exit if no other non-daemon threads are running.
     * Callers are responsible for keeping the JVM alive (e.g., by using
     * {@code spring.main.keep-alive=true} in Spring Boot applications).
     * <p>
     * This method is suitable for integration with lifecycle managers.
     *
     * @throws IOException In case of I/O error during server startup.
     */
    @Override
    public void startAsync() throws IOException {
        startServerInternal();
    }

    /**
     * Creates and starts the gRPC server if it is not already running.
     * If the server is already running, returns the existing instance.
     * Acquires the lock internally to protect shared state ({@code server}, {@code healthManager}).
     *
     * @return the running {@link Server} instance (either newly created or existing).
     * @throws IOException if the server fails to start.
     */
    private Server startServerInternal() throws IOException {
        warnIfHeapDumpOptionsNotSet();

        lock.lock();
        MonitoringHttpServer newMonitoringServer = null;
        MetricsContextSnapshot newMetricsSnapshot = null;
        Server newServer = null;
        boolean started = false;
        try {
            if (server != null) {
                started = true;
                return server;
            }
            int port = config.port();
            int monitoringPort = config.monitoringPort();

            // Build the metrics snapshot once: it installs common tags on the MeterRegistry
            // and binds JVM meters. Binding twice would double-register meters and fail.
            newMetricsSnapshot = new MetricsContextSnapshot(
                    metricsContext,
                    config.pipelinePath(),
                    config.clusterUrl()
            );

            log.info("Starting monitoring HTTP server on port {}", monitoringPort);
            newMonitoringServer = new MonitoringHttpServer(monitoringPort, httpHandlers);
            newMonitoringServer.start();
            log.info("Monitoring HTTP server started on port {}", newMonitoringServer.getPort());

            log.info("Starting gRPC server on port {}", port);

            HealthStatusManager newHealthManager = new HealthStatusManager();
            newServer = ServerBuilder.forPort(port)
                    .maxInboundMessageSize(Integer.MAX_VALUE)
                    .addService(new CompanionService(pipelineContextSnapshot, jobContext))
                    .addService(newHealthManager.getHealthService())
                    .build()
                    .start();
            newHealthManager.setStatus("", HealthCheckResponse.ServingStatus.SERVING);

            this.monitoringServer = newMonitoringServer;
            this.server = newServer;
            this.healthManager = newHealthManager;
            this.metricsSnapshot = newMetricsSnapshot;

            log.info("gRPC server started successfully on port {}", port);
            started = true;
            return newServer;
        } finally {
            // If start did not complete (any exception, checked or unchecked, thrown after the
            // gRPC server was started, the monitoring server was started, or the metrics snapshot
            // was built), roll those back.
            if (!started) {
                if (newServer != null) {
                    // The gRPC server may already be started.
                    // It has no in-flight RPCs yet, so force shutdown.
                    newServer.shutdownNow();
                }
                stopMonitoringServerQuietly(newMonitoringServer);
                closeMetricsSnapshotQuietly(newMetricsSnapshot);
            }
            lock.unlock();
        }
    }

    /**
     * Stops the monitoring HTTP server if present, swallowing any {@link RuntimeException}
     * and logging a warning. A no-op when {@code monitoringServer} is {@code null}.
     *
     * @return {@code true} if the server was stopped without error (or was {@code null}),
     * {@code false} if stopping it threw.
     */
    private static boolean stopMonitoringServerQuietly(@Nullable MonitoringHttpServer monitoringServer) {
        if (monitoringServer == null) {
            return true;
        }
        try {
            monitoringServer.stop();
            return true;
        } catch (RuntimeException stopFailure) {
            log.warn("Failed to stop monitoring HTTP server", stopFailure);
            return false;
        }
    }

    /**
     * Closes the {@link MetricsContextSnapshot} if present, swallowing any {@link RuntimeException}
     * and logging a warning. A no-op when {@code metricsSnapshot} is {@code null}.
     */
    private static void closeMetricsSnapshotQuietly(@Nullable MetricsContextSnapshot metricsSnapshot) {
        if (metricsSnapshot != null) {
            try {
                metricsSnapshot.close();
            } catch (RuntimeException closeFailure) {
                log.warn("Failed to close MetricsContextSnapshot", closeFailure);
            }
        }
    }

    /**
     * Stop the GRPC server gracefully.
     * <p>
     * This method first sets the health status to NOT_SERVING, then initiates
     * a graceful shutdown. If the server doesn't terminate within the timeout,
     * it forces shutdown.
     * <p>
     * Also stops the monitoring HTTP server and closes the {@link MetricsContextSnapshot}
     * built at start, which releases the JVM binders. The underlying
     * {@link io.micrometer.core.instrument.MeterRegistry} is owned by the caller and is
     * not closed here.
     */
    @Override
    public void stop() {
        MonitoringHttpServer localMonitoringServer;
        Server localServer;
        HealthStatusManager localHealthManager;
        MetricsContextSnapshot localMetricsSnapshot;

        lock.lock();
        try {
            if (server == null) {
                return;
            }
            localMonitoringServer = monitoringServer;
            localServer = server;
            localHealthManager = healthManager;
            localMetricsSnapshot = metricsSnapshot;
            monitoringServer = null;
            server = null;
            healthManager = null;
            metricsSnapshot = null;
        } finally {
            lock.unlock();
        }

        if (localMonitoringServer != null) {
            log.info("Stopping monitoring HTTP server...");
            if (stopMonitoringServerQuietly(localMonitoringServer)) {
                log.info("Monitoring HTTP server stopped");
            }
        }

        log.info("Stopping gRPC server...");
        if (localHealthManager != null) {
            localHealthManager.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        }
        localServer.shutdown();
        try {
            if (!localServer.awaitTermination(DEFAULT_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("gRPC server did not terminate gracefully within {} seconds, forcing shutdown",
                        DEFAULT_SHUTDOWN_TIMEOUT_SECONDS);
                localServer.shutdownNow();
                if (!localServer.awaitTermination(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.error("gRPC server did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for gRPC server shutdown, forcing shutdown");
            localServer.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("gRPC server stopped");

        if (localMetricsSnapshot != null) {
            try {
                localMetricsSnapshot.close();
            } catch (RuntimeException e) {
                log.warn("Failed to close MetricsContextSnapshot", e);
            }
        }
    }

    /**
     * Returns whether the server is currently running.
     * <p>
     * This method checks both that the server instance exists and that it has not been
     * shut down (e.g., due to an internal error in the gRPC transport layer).
     *
     * @return true if the server is running, false otherwise.
     */
    @Override
    public boolean isRunning() {
        lock.lock();
        try {
            return server != null && !server.isShutdown();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the port on which the gRPC server is listening.
     * <p>
     * This method is useful for testing when using port 0 (auto-assigned port).
     *
     * @return The server port, or -1 if the server is not running.
     */
    @Override
    public int getPort() {
        lock.lock();
        try {
            return (server != null && !server.isShutdown()) ? server.getPort() : -1;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the port the monitoring HTTP server is listening on, or {@code -1}
     * if it is not running. Useful in tests with port 0 (OS-assigned).
     */
    @Override
    public int getMonitoringPort() {
        lock.lock();
        try {
            return monitoringServer != null ? monitoringServer.getPort() : -1;
        } finally {
            lock.unlock();
        }
    }

    private static void warnIfHeapDumpOptionsNotSet() {
        List<String> jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();

        boolean hasHeapDumpOnOom = jvmArgs.stream()
                .anyMatch(arg -> arg.contains("-XX:+HeapDumpOnOutOfMemoryError"));
        boolean hasHeapDumpPath = jvmArgs.stream()
                .anyMatch(arg -> arg.contains("-XX:HeapDumpPath"));

        if (!hasHeapDumpOnOom || !hasHeapDumpPath) {
            log.warn("Heap dump JVM options are not configured. "
                            + "It is recommended to set -XX:+HeapDumpOnOutOfMemoryError and -XX:HeapDumpPath=<path> "
                            + "via YT_FLOW_COMPANION_JVM_EXTRA_OPTS environment variable "
                            + "to enable heap dump generation on OutOfMemoryError "
                            + "(HasHeapDumpOnOutOfMemoryError: {}, HasHeapDumpPath: {}, JvmArgs: {})",
                    hasHeapDumpOnOom, hasHeapDumpPath, jvmArgs);
        }
    }
}
