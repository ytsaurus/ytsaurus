package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.ytsaurus.flow.context.MetricsContextSnapshot;
import tech.ytsaurus.flow.service.CompanionService;
import tech.ytsaurus.flow.service.ResourceStore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GrpcCompanionServerStarterTest {

    @Test
    void startsOneCompleteRuntimeInOrder() throws IOException {
        RecordingComponentFactory components = new RecordingComponentFactory(null, null);
        var starter = new GrpcCompanionServerStarter(components);

        CompanionServerRuntime runtime = starter.start();

        assertInstanceOf(GrpcCompanionServerRuntime.class, runtime);
        assertEquals(1, components.metricsCreations);
        assertEquals(List.of(
                "metrics.create",
                "monitoring.create",
                "monitoring.start",
                "health.create",
                "resources.create",
                "companion.create",
                "grpc.build",
                "grpc.start",
                "health.serving"
        ), components.events);
    }

    @ParameterizedTest(name = "failure at {0}")
    @EnumSource(StartStep.class)
    void rollsBackEveryFailedConstructionStepInReverseOrder(StartStep failedStep) {
        RuntimeException startFailure = new RuntimeException("failed at " + failedStep);
        RecordingComponentFactory components = new RecordingComponentFactory(failedStep, startFailure);
        var starter = new GrpcCompanionServerStarter(components);

        RuntimeException exception = assertThrows(RuntimeException.class, starter::start);

        assertSame(startFailure, exception);
        assertEquals(expectedEvents(failedStep), components.events);
    }

    @Test
    void rollbackContinuesAndSuppressesEveryCleanupFailure() {
        RuntimeException startFailure = new RuntimeException("publish failed");
        RuntimeException healthFailure = new RuntimeException("health rollback failed");
        AssertionError grpcFailure = new AssertionError("gRPC rollback failed");
        RuntimeException companionFailure = new RuntimeException("companion rollback failed");
        RuntimeException monitoringFailure = new RuntimeException("monitoring rollback failed");
        AssertionError metricsFailure = new AssertionError("metrics rollback failed");
        RecordingComponentFactory components = new RecordingComponentFactory(StartStep.SERVING, startFailure);
        components.cleanupFailures.put("health.not_serving", healthFailure);
        components.cleanupFailures.put("grpc.shutdown_now", grpcFailure);
        components.cleanupFailures.put("resources.shutdown", companionFailure);
        components.cleanupFailures.put("monitoring.stop", monitoringFailure);
        components.cleanupFailures.put("metrics.close", metricsFailure);
        var starter = new GrpcCompanionServerStarter(components);

        AssertionError exception = assertThrows(AssertionError.class, starter::start);

        assertSame(grpcFailure, exception);
        assertArrayEquals(new Throwable[]{healthFailure}, startFailure.getSuppressed());
        assertArrayEquals(
                new Throwable[]{startFailure, companionFailure, monitoringFailure, metricsFailure},
                grpcFailure.getSuppressed()
        );
        assertEquals(List.of(
                "metrics.create",
                "monitoring.create",
                "monitoring.start",
                "health.create",
                "resources.create",
                "companion.create",
                "grpc.build",
                "grpc.start",
                "health.serving",
                "health.not_serving",
                "grpc.shutdown_now",
                "resources.shutdown",
                "monitoring.stop",
                "metrics.close"
        ), components.events);
    }

    @Test
    void startupVirtualMachineErrorEscapesAfterRollback() {
        OutOfMemoryError failure = new OutOfMemoryError("fatal startup failure");
        RecordingComponentFactory components = new RecordingComponentFactory(StartStep.GRPC_START, failure);
        var starter = new GrpcCompanionServerStarter(components);

        assertSame(failure, assertThrows(OutOfMemoryError.class, starter::start));
        assertEquals(expectedEvents(StartStep.GRPC_START), components.events);
    }

    @Test
    void rollbackVirtualMachineErrorEscapesAfterAllActions() {
        RuntimeException startFailure = new RuntimeException("publish failed");
        OutOfMemoryError fatalFailure = new OutOfMemoryError("fatal rollback failure");
        RuntimeException monitoringFailure = new RuntimeException("monitoring rollback failed");
        RuntimeException metricsFailure = new RuntimeException("metrics rollback failed");
        RecordingComponentFactory components = new RecordingComponentFactory(StartStep.SERVING, startFailure);
        components.cleanupFailures.put("grpc.shutdown_now", fatalFailure);
        components.cleanupFailures.put("monitoring.stop", monitoringFailure);
        components.cleanupFailures.put("metrics.close", metricsFailure);
        var starter = new GrpcCompanionServerStarter(components);

        assertSame(fatalFailure, assertThrows(OutOfMemoryError.class, starter::start));
        assertArrayEquals(new Throwable[]{startFailure, monitoringFailure, metricsFailure},
                fatalFailure.getSuppressed());
        assertEquals(0, startFailure.getSuppressed().length);
        assertEquals(expectedEvents(StartStep.SERVING), components.events);
    }

    private static List<String> expectedEvents(StartStep failedStep) {
        List<String> events = new ArrayList<>(List.of(
                "metrics.create",
                "monitoring.create",
                "monitoring.start",
                "health.create",
                "resources.create",
                "companion.create",
                "grpc.build",
                "grpc.start",
                "health.serving"
        ).subList(0, failedStep.ordinal() + 1));
        if (failedStep == StartStep.SERVING) {
            events.add("health.not_serving");
        }
        if (failedStep.ordinal() >= StartStep.GRPC_START.ordinal()) {
            events.add("grpc.shutdown_now");
        }
        if (failedStep.ordinal() >= StartStep.COMPANION_SERVICE.ordinal()) {
            events.add("resources.shutdown");
        }
        if (failedStep.ordinal() >= StartStep.MONITORING_START.ordinal()) {
            events.add("monitoring.stop");
        }
        if (failedStep != StartStep.METRICS) {
            events.add("metrics.close");
        }
        return events;
    }

    private enum StartStep {
        METRICS,
        MONITORING_CONSTRUCTION,
        MONITORING_START,
        HEALTH,
        RESOURCES,
        COMPANION_SERVICE,
        GRPC_BUILD,
        GRPC_START,
        SERVING
    }

    private static final class RecordingComponentFactory
            implements GrpcCompanionServerStarter.ComponentFactory {
        private final List<String> events = new ArrayList<>();
        private final Map<String, Throwable> cleanupFailures = new HashMap<>();
        private final StartStep failedStep;
        private final Throwable startFailure;
        private final MetricsContextSnapshot metricsSnapshot = mock(MetricsContextSnapshot.class);
        private final MonitoringHttpServer monitoringServer = mock(MonitoringHttpServer.class);
        private final HealthStatusManager healthManager = mock(HealthStatusManager.class);
        private final CompanionService companionService = mock(CompanionService.class);
        private final ResourceStore resources = mock(ResourceStore.class);
        private final Server grpcServer = mock(Server.class);
        private int metricsCreations;

        private RecordingComponentFactory(StartStep failedStep, Throwable startFailure) {
            this.failedStep = failedStep;
            this.startFailure = startFailure;
            when(grpcServer.getPort()).thenReturn(12345);
            when(monitoringServer.getPort()).thenReturn(23456);
            doAnswer(invocation -> {
                cleanup("metrics.close");
                return null;
            }).when(metricsSnapshot).close();
            doAnswer(invocation -> {
                cleanup("monitoring.stop");
                return null;
            }).when(monitoringServer).stop();
            doAnswer(invocation -> {
                cleanup("resources.shutdown");
                return true;
            }).when(resources).shutdown();
            doAnswer(invocation -> {
                cleanup("grpc.shutdown_now");
                return grpcServer;
            }).when(grpcServer).shutdownNow();
            doAnswer(invocation -> {
                startStep(StartStep.SERVING, "health.serving");
                return null;
            }).when(healthManager).setStatus("", HealthCheckResponse.ServingStatus.SERVING);
            doAnswer(invocation -> {
                cleanup("health.not_serving");
                return null;
            }).when(healthManager).setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
            try {
                doAnswer(invocation -> {
                    startStep(StartStep.MONITORING_START, "monitoring.start");
                    return null;
                }).when(monitoringServer).start();
                doAnswer(invocation -> {
                    startStep(StartStep.GRPC_START, "grpc.start");
                    return grpcServer;
                }).when(grpcServer).start();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public MetricsContextSnapshot createMetricsSnapshot() {
            metricsCreations++;
            startStep(StartStep.METRICS, "metrics.create");
            return metricsSnapshot;
        }

        @Override
        public MonitoringHttpServer createMonitoringServer() {
            startStep(StartStep.MONITORING_CONSTRUCTION, "monitoring.create");
            return monitoringServer;
        }

        @Override
        public HealthStatusManager createHealthManager() {
            startStep(StartStep.HEALTH, "health.create");
            return healthManager;
        }

        @Override
        public ResourceStore createResourceStore() {
            startStep(StartStep.RESOURCES, "resources.create");
            return resources;
        }

        @Override
        public CompanionService createCompanionService(MetricsContextSnapshot ignored, ResourceStore resourceStore) {
            assertSame(resources, resourceStore);
            startStep(StartStep.COMPANION_SERVICE, "companion.create");
            return companionService;
        }

        @Override
        public Server buildGrpcServer(CompanionService ignored, HealthStatusManager ignoredHealthManager) {
            startStep(StartStep.GRPC_BUILD, "grpc.build");
            return grpcServer;
        }

        private void startStep(StartStep step, String event) {
            events.add(event);
            if (failedStep == step) {
                throwUnchecked(startFailure);
            }
        }

        private void cleanup(String event) {
            events.add(event);
            Throwable failure = cleanupFailures.get(event);
            if (failure != null) {
                throwUnchecked(failure);
            }
        }

        private static void throwUnchecked(Throwable failure) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(failure);
        }
    }
}
