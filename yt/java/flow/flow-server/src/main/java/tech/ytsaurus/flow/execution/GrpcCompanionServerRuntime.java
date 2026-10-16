package tech.ytsaurus.flow.execution;

import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.context.MetricsContextSnapshot;
import tech.ytsaurus.flow.internal.utils.FailureCollector;
import tech.ytsaurus.flow.service.ResourceStore;

/**
 * Owns one in-process companion server runtime and performs ordered try-all shutdown.
 */
final class GrpcCompanionServerRuntime implements CompanionServerRuntime {
    private static final Logger log = LoggerFactory.getLogger(GrpcCompanionServerRuntime.class);
    private static final int GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 30;
    private static final int FORCED_SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final MonitoringHttpServer monitoringServer;
    private final Server grpcServer;
    private final ResourceStore resources;
    private final HealthStatusManager healthManager;
    private final MetricsContextSnapshot metricsSnapshot;

    GrpcCompanionServerRuntime(
            MonitoringHttpServer monitoringServer,
            Server grpcServer,
            ResourceStore resources,
            HealthStatusManager healthManager,
            MetricsContextSnapshot metricsSnapshot
    ) {
        this.monitoringServer = monitoringServer;
        this.grpcServer = grpcServer;
        this.resources = resources;
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
        FailureCollector failures = new FailureCollector();
        failures.tryRun(monitoringServer::stop);
        failures.tryRun(() -> healthManager.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING));
        boolean interrupted = shutdownGrpc(failures);
        failures.tryRun(this::shutdownResources);
        // MetricsContextSnapshot does not own the caller's MeterRegistry.
        failures.tryRun(metricsSnapshot::close);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        failures.throwUncheckedIfAny();
    }

    private void shutdownResources() {
        if (!resources.shutdown()) {
            log.warn("Companion resources are still in use after server shutdown; "
                    + "their unload hooks will run when the outstanding requests finish");
        }
    }

    private boolean shutdownGrpc(FailureCollector failures) {
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

}
