package tech.ytsaurus.flow.execution;

import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import tech.ytsaurus.flow.context.MetricsContextSnapshot;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcCompanionServerRuntimeTest {
    private MonitoringHttpServer monitoringServer;
    private Server grpcServer;
    private HealthStatusManager healthManager;
    private MetricsContextSnapshot metricsSnapshot;
    private GrpcCompanionServerRuntime runtime;

    @BeforeEach
    void setUp() {
        monitoringServer = mock(MonitoringHttpServer.class);
        grpcServer = mock(Server.class);
        healthManager = mock(HealthStatusManager.class);
        metricsSnapshot = mock(MetricsContextSnapshot.class);
        when(grpcServer.shutdown()).thenReturn(grpcServer);
        when(grpcServer.shutdownNow()).thenReturn(grpcServer);
        runtime = new GrpcCompanionServerRuntime(
                monitoringServer,
                grpcServer,
                healthManager,
                metricsSnapshot
        );
    }

    @Test
    void exposesOwnedServersAndAwaitsGrpcTermination() throws InterruptedException {
        when(grpcServer.isShutdown()).thenReturn(false, true);
        when(grpcServer.getPort()).thenReturn(12345);
        when(monitoringServer.getPort()).thenReturn(23456);

        assertTrue(runtime.isServing());
        assertFalse(runtime.isServing());
        assertEquals(12345, runtime.grpcPort());
        assertEquals(23456, runtime.monitoringPort());

        runtime.awaitTermination();

        verify(grpcServer).awaitTermination();
    }

    @Test
    void gracefulShutdownStopsEveryOwnedComponentInOrder() throws InterruptedException {
        when(grpcServer.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(true);

        runtime.shutdown();

        InOrder order = inOrder(monitoringServer, healthManager, grpcServer, metricsSnapshot);
        order.verify(monitoringServer).stop();
        order.verify(healthManager).setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        order.verify(grpcServer).shutdown();
        order.verify(grpcServer).awaitTermination(30, TimeUnit.SECONDS);
        order.verify(metricsSnapshot).close();
        verify(grpcServer, never()).shutdownNow();
    }

    @Test
    void forcedShutdownWaitsAfterShutdownNow() throws InterruptedException {
        when(grpcServer.awaitTermination(30, TimeUnit.SECONDS)).thenReturn(false);
        when(grpcServer.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(true);

        runtime.shutdown();

        InOrder order = inOrder(monitoringServer, healthManager, grpcServer, metricsSnapshot);
        order.verify(monitoringServer).stop();
        order.verify(healthManager).setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        order.verify(grpcServer).shutdown();
        order.verify(grpcServer).awaitTermination(30, TimeUnit.SECONDS);
        order.verify(grpcServer).shutdownNow();
        order.verify(grpcServer).awaitTermination(10, TimeUnit.SECONDS);
        order.verify(metricsSnapshot).close();
    }

    @Test
    void shutdownStillClosesMetricsWhenForcedShutdownDoesNotTerminate() throws InterruptedException {
        when(grpcServer.awaitTermination(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(false);

        runtime.shutdown();

        verify(grpcServer).shutdownNow();
        verify(grpcServer, times(2)).awaitTermination(anyLong(), eq(TimeUnit.SECONDS));
        verify(metricsSnapshot).close();
    }

    @Test
    void interruptionForcesShutdownRestoresFlagAndStillClosesMetrics() throws InterruptedException {
        when(grpcServer.awaitTermination(30, TimeUnit.SECONDS))
                .thenThrow(new InterruptedException("stop interrupted"));
        assertFalse(Thread.interrupted());
        try {
            runtime.shutdown();

            assertTrue(Thread.currentThread().isInterrupted());
            verify(grpcServer).shutdownNow();
            verify(grpcServer, times(1)).awaitTermination(anyLong(), eq(TimeUnit.SECONDS));
            verify(metricsSnapshot).close();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void shutdownContinuesAfterEveryThrowableAndRethrowsTheFirst() throws InterruptedException {
        AssertionError monitoringFailure = new AssertionError("monitoring failed");
        RuntimeException healthFailure = new RuntimeException("health failed");
        AssertionError shutdownFailure = new AssertionError("shutdown failed");
        RuntimeException metricsFailure = new RuntimeException("metrics failed");
        doThrow(monitoringFailure).when(monitoringServer).stop();
        doThrow(healthFailure).when(healthManager)
                .setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        when(grpcServer.shutdown()).thenThrow(shutdownFailure);
        when(grpcServer.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(true);
        doThrow(metricsFailure).when(metricsSnapshot).close();

        assertSame(monitoringFailure, assertThrows(AssertionError.class, runtime::shutdown));

        assertArrayEquals(
                new Throwable[]{healthFailure, shutdownFailure, metricsFailure},
                monitoringFailure.getSuppressed()
        );
        verify(grpcServer).shutdownNow();
        verify(grpcServer).awaitTermination(10, TimeUnit.SECONDS);
        verify(metricsSnapshot).close();
    }
}
