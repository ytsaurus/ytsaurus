package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

import com.sun.net.httpserver.HttpHandler;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.MetricsContext;
import tech.ytsaurus.flow.context.MetricsContextSnapshot;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.internal.utils.FailureCollector;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.service.CompanionRequestProcessor;
import tech.ytsaurus.flow.service.CompanionService;
import tech.ytsaurus.flow.service.JobSpecValidator;
import tech.ytsaurus.flow.service.ResourceStore;

/**
 * Starts a complete companion server runtime and rolls back partial startup.
 */
final class GrpcCompanionServerStarter implements CompanionServerRuntime.Starter {
    private static final Logger log = LoggerFactory.getLogger(GrpcCompanionServerStarter.class);

    private final ComponentFactory components;

    GrpcCompanionServerStarter(
            CompanionExecutionConfig config,
            PipelineContextSnapshot pipelineContext,
            JobContext jobContext,
            MetricsContext metricsContext,
            Map<String, HttpHandler> httpHandlers,
            JobSpecValidator jobSpecValidator
    ) {
        this(new ProductionComponentFactory(
                config,
                pipelineContext,
                jobContext,
                metricsContext,
                httpHandlers,
                jobSpecValidator
        ));
    }

    GrpcCompanionServerStarter(ComponentFactory components) {
        this.components = components;
    }

    @Override
    public CompanionServerRuntime start() throws IOException {
        Deque<Runnable> rollbackActions = new ArrayDeque<>();
        try {
            MetricsContextSnapshot metrics = components.createMetricsSnapshot();
            rollbackActions.push(metrics::close);

            MonitoringHttpServer monitoring = components.createMonitoringServer();
            rollbackActions.push(monitoring::stop);
            monitoring.start();

            HealthStatusManager health = components.createHealthManager();
            ResourceStore resources = components.createResourceStore();
            rollbackActions.push(resources::shutdown);
            CompanionService companion = components.createCompanionService(metrics, resources);
            Server grpc = components.buildGrpcServer(companion, health);
            // Keep the built server before binding so a partially successful start can be rolled back.
            rollbackActions.push(grpc::shutdownNow);
            grpc.start();

            // Register the inverse before publishing the externally visible serving state.
            rollbackActions.push(() -> health.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING));
            health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);

            log.info("Companion server started on ports {}/{}", grpc.getPort(), monitoring.getPort());
            return new GrpcCompanionServerRuntime(monitoring, grpc, resources, health, metrics);
        } catch (Throwable startFailure) {
            throw rollback(startFailure, rollbackActions);
        }
    }

    private static IOException rollback(Throwable startFailure, Deque<Runnable> actions) {
        var failures = new FailureCollector();
        failures.add(startFailure);
        while (!actions.isEmpty()) {
            failures.tryRun(actions.pop());
        }
        return asIOExceptionOrThrowUnchecked(failures.asException());
    }

    private static IOException asIOExceptionOrThrowUnchecked(Throwable failure) {
        if (failure instanceof IOException exception) {
            return exception;
        }
        if (failure instanceof RuntimeException exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new IOException("Failed to start companion server", failure);
    }

    /**
     * Creates construction-only components while the starter owns their lifecycle side effects.
     */
    interface ComponentFactory {
        MetricsContextSnapshot createMetricsSnapshot();

        MonitoringHttpServer createMonitoringServer();

        HealthStatusManager createHealthManager();

        ResourceStore createResourceStore();

        CompanionService createCompanionService(MetricsContextSnapshot metricsSnapshot, ResourceStore resources);

        Server buildGrpcServer(CompanionService companionService, HealthStatusManager healthManager);
    }

    private static final class ProductionComponentFactory implements ComponentFactory {
        private final CompanionExecutionConfig config;
        private final PipelineContextSnapshot pipelineContext;
        private final JobContext jobContext;
        private final MetricsContext metricsContext;
        private final Map<String, HttpHandler> httpHandlers;
        private final JobSpecValidator jobSpecValidator;

        private ProductionComponentFactory(
                CompanionExecutionConfig config,
                PipelineContextSnapshot pipelineContext,
                JobContext jobContext,
                MetricsContext metricsContext,
                Map<String, HttpHandler> httpHandlers,
                JobSpecValidator jobSpecValidator
        ) {
            this.config = config;
            this.pipelineContext = pipelineContext;
            this.jobContext = jobContext;
            this.metricsContext = metricsContext;
            this.httpHandlers = httpHandlers;
            this.jobSpecValidator = jobSpecValidator;
        }

        @Override
        public MetricsContextSnapshot createMetricsSnapshot() {
            return new MetricsContextSnapshot(metricsContext, config.pipelinePath(), config.clusterUrl());
        }

        @Override
        public MonitoringHttpServer createMonitoringServer() {
            return new MonitoringHttpServer(config.monitoringPort(), httpHandlers);
        }

        @Override
        public HealthStatusManager createHealthManager() {
            return new HealthStatusManager();
        }

        @Override
        public ResourceStore createResourceStore() {
            return new ResourceStore(pipelineContext.getResourceFactories());
        }

        @Override
        public CompanionService createCompanionService(
                MetricsContextSnapshot metricsSnapshot,
                ResourceStore resources
        ) {
            return new CompanionService(
                    new CompanionRequestProcessor(
                            pipelineContext,
                            jobContext,
                            resources,
                            jobSpecValidator
                    ),
                    metricsSnapshot.getRegistry()
            );
        }

        @Override
        public Server buildGrpcServer(CompanionService companionService, HealthStatusManager healthManager) {
            return ServerBuilder.forPort(config.port())
                    .maxInboundMessageSize(Integer.MAX_VALUE)
                    .addService(companionService)
                    .addService(healthManager.getHealthService())
                    .build();
        }
    }
}
