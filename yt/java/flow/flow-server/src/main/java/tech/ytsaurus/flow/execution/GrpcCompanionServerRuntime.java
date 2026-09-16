package tech.ytsaurus.flow.execution;

import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.context.MetricsContextSnapshot;

/**
 * Owns one in-process companion server runtime and performs ordered try-all shutdown.
 */
final class GrpcCompanionServerRuntime implements CompanionServerRuntime {
    private static final Logger log = LoggerFactory.getLogger(GrpcCompanionServerRuntime.class);
    private static final int GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 30;
    private static final int FORCED_SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final MonitoringHttpServer monitoringServer;
    private final Server grpcServer;
    private final HealthStatusManager healthManager;
    private final MetricsContextSnapshot metricsSnapshot;

    GrpcCompanionServerRuntime(
            MonitoringHttpServer monitoringServer,
            Server grpcServer,
            HealthStatusManager healthManager,
            MetricsContextSnapshot metricsSnapshot
    ) {
        this.monitoringServer = monitoringServer;
        this.grpcServer = grpcServer;
        this.healthManager = healthManager;
        this.metricsSnapshot = metricsSnapshot;
    }

    @Override
    public boolean isServing() {
        return !grpcServer.isShutdown();
    }

    @Override
    public int grpcPort() {
        return grpcServer.getPort();
    }

    @Override
    public int monitoringPort() {
        return monitoringServer.getPort();
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    @Override
    public void shutdown() {
        ShutdownFailures failures = new ShutdownFailures();
        failures.tryRun(monitoringServer::stop);
        failures.tryRun(() -> healthManager.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING));
        boolean interrupted = shutdownGrpc(failures);
        // MetricsContextSnapshot does not own the caller's MeterRegistry.
        failures.tryRun(metricsSnapshot::close);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        failures.throwIfAny();
    }

    private boolean shutdownGrpc(ShutdownFailures failures) {
        boolean interrupted = false;
        boolean terminated = false;
        if (failures.tryRun(grpcServer::shutdown)) {
            try {
                terminated = grpcServer.awaitTermination(GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (Throwable e) {
                failures.add(e);
            }
        }

        if (!terminated) {
            log.warn("Companion server did not terminate gracefully; forcing shutdown");
            boolean forceRequested = failures.tryRun(grpcServer::shutdownNow);
            if (forceRequested && !interrupted) {
                try {
                    if (!grpcServer.awaitTermination(FORCED_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        log.error("Companion server did not terminate after forced shutdown");
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Throwable e) {
                    failures.add(e);
                }
            }
        }
        return interrupted;
    }

    private static final class ShutdownFailures {
        private @Nullable Throwable failure;

        boolean tryRun(Runnable action) {
            try {
                action.run();
                return true;
            } catch (Throwable e) {
                add(e);
                return false;
            }
        }

        void add(Throwable next) {
            if (failure == null) {
                failure = next;
            } else if (failure != next) {
                failure.addSuppressed(next);
            }
        }

        void throwIfAny() {
            if (failure instanceof RuntimeException exception) {
                throw exception;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure != null) {
                throw new IllegalStateException("Unexpected checked shutdown failure", failure);
            }
        }
    }
}
