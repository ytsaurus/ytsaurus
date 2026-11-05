package tech.ytsaurus.flow.context;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jspecify.annotations.Nullable;

/**
 * Build-phase configuration container for a Flow pipeline.
 *
 * <p>{@link MetricsContext} is a plain configuration carrier: it captures the
 * {@link MeterRegistry} the user wants Flow to publish to, and whether the framework
 * should bind JVM meters. It has no side effects on construction and owns no resources.
 *
 * <p>The configuration is consumed by {@link MetricsContextSnapshot}, which the framework
 * builds at server start. The snapshot is what actually mutates the registry — installing
 * pipeline-identifying common tags and binding JVM binders — and owns the lifecycle of
 * those bindings.
 */
public final class MetricsContext {
    private final MeterRegistry meterRegistry;
    private final boolean jvmMetricsEnabled;

    MetricsContext(Builder builder) {
        this.meterRegistry = builder.meterRegistry;
        this.jvmMetricsEnabled = builder.jvmMetricsEnabled;
    }

    public static Builder builder() {
        return new Builder();
    }

    public MeterRegistry getRegistry() {
        return meterRegistry;
    }

    public boolean isJvmMetricsEnabled() {
        return jvmMetricsEnabled;
    }

    public static class Builder {
        private @Nullable MeterRegistry meterRegistry;
        private boolean jvmMetricsEnabled = true;

        public Builder withJvmMetrics(boolean enabled) {
            this.jvmMetricsEnabled = enabled;
            return this;
        }

        public Builder withMeterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            return this;
        }

        public MetricsContext build() {
            // Materialize the default registry lazily: builder() must not allocate a
            // SimpleMeterRegistry that a subsequent withMeterRegistry(...) would discard.
            if (meterRegistry == null) {
                meterRegistry = new SimpleMeterRegistry();
            }
            return new MetricsContext(this);
        }
    }
}
