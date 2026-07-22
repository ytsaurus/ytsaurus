package tech.ytsaurus.flow.context;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("MetricsContext Tests")
class MetricsContextTest {

    @Test
    @DisplayName("Default builder creates context with a SimpleMeterRegistry and JVM metrics enabled")
    void testDefaultBuilder() {
        var ctx = MetricsContext.builder().build();
        assertAll(
                () -> assertNotNull(ctx.getRegistry()),
                () -> assertTrue(ctx.isJvmMetricsEnabled())
        );
    }

    @Test
    @DisplayName("Builder with JVM metrics disabled")
    void testJvmMetricsDisabled() {
        var ctx = MetricsContext.builder()
                .withJvmMetrics(false)
                .build();
        assertFalse(ctx.isJvmMetricsEnabled());
    }

    @Test
    @DisplayName("Builder with custom MeterRegistry")
    void testCustomMeterRegistry() {
        MeterRegistry customRegistry = new SimpleMeterRegistry();
        MetricsContext ctx = MetricsContext.builder()
                .withMeterRegistry(customRegistry)
                .build();
        assertEquals(customRegistry, ctx.getRegistry());
    }

    @Test
    @DisplayName("Building does not register any meters on the configured registry")
    void testBuildHasNoSideEffectsOnRegistry() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsContext.builder()
                .withMeterRegistry(registry)
                .withJvmMetrics(true)
                .build();
        assertTrue(registry.getMeters().isEmpty(),
                "MetricsContext is pure config; building it must not touch the registry");
    }
}
