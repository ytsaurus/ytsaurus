package tech.ytsaurus.flow.context;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("MetricsContextSnapshot Tests")
class MetricsContextSnapshotTest {

    private static final String PIPELINE_PATH = "//tmp/pipeline";
    private static final String CLUSTER_URL = "test-cluster";

    @Nested
    @DisplayName("JVM Metrics Binding")
    class JvmMetricsTests {

        @Test
        @DisplayName("JVM metrics are bound when enabled")
        void testJvmMetricsEnabled() {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            MetricsContext config = MetricsContext.builder()
                    .withMeterRegistry(registry)
                    .withJvmMetrics(true)
                    .build();
            try (var snapshot = new MetricsContextSnapshot(config, PIPELINE_PATH, CLUSTER_URL)) {
                assertNotNull(snapshot.getRegistry());
                assertFalse(registry.getMeters().isEmpty(),
                        "JVM meters should be registered when JVM metrics are enabled");
            }
        }

        @Test
        @DisplayName("JVM metrics are not bound when disabled")
        void testJvmMetricsDisabled() {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            MetricsContext config = MetricsContext.builder()
                    .withMeterRegistry(registry)
                    .withJvmMetrics(false)
                    .build();
            try (var snapshot = new MetricsContextSnapshot(config, PIPELINE_PATH, CLUSTER_URL)) {
                assertNotNull(snapshot.getRegistry());
                assertTrue(registry.getMeters().isEmpty(),
                        "No JVM meters should be registered when JVM metrics are disabled");
            }
        }
    }

    @Nested
    @DisplayName("Common Tag Installation")
    class CommonTagsTests {

        @Test
        @DisplayName("Common tags are applied to JVM meters bound by the snapshot")
        void testTagsAppliedToJvmMeters() {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            MetricsContext config = MetricsContext.builder()
                    .withMeterRegistry(registry)
                    .withJvmMetrics(true)
                    .build();
            try (var ignored = new MetricsContextSnapshot(config, PIPELINE_PATH, CLUSTER_URL)) {
                assertFalse(registry.getMeters().isEmpty());
                for (Meter meter : registry.getMeters()) {
                    assertEquals(PIPELINE_PATH, meter.getId().getTag("pipeline_path"),
                            "Every meter bound by the snapshot must carry pipeline_path");
                    assertEquals(CLUSTER_URL, meter.getId().getTag("pipeline_cluster"),
                            "Every meter bound by the snapshot must carry pipeline_cluster");
                }
            }
        }

        @Test
        @DisplayName("Common tags are applied to meters registered after snapshot construction")
        void testTagsAppliedToUserMetersRegisteredAfterSnapshot() {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            MetricsContext config = MetricsContext.builder()
                    .withMeterRegistry(registry)
                    .withJvmMetrics(false)
                    .build();
            try (var ignored = new MetricsContextSnapshot(config, PIPELINE_PATH, CLUSTER_URL)) {
                var counter = registry.counter("user.counter");
                assertEquals(PIPELINE_PATH, counter.getId().getTag("pipeline_path"));
                assertEquals(CLUSTER_URL, counter.getId().getTag("pipeline_cluster"));
            }
        }
    }
}
