package tech.ytsaurus.flow.context;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadDeadlockMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import org.jspecify.annotations.Nullable;

/**
 * Applied runtime view of a {@link MetricsContext} constructed at server start.
 *
 * <p><b>Ownership.</b> The snapshot owns only the bindings it creates here (the JVM binders).
 * It does <em>not</em> own the {@link MeterRegistry}: the registry is supplied through
 * {@link MetricsContext} by the caller (e.g. a Spring-managed bean), so {@link #close()}
 * releases the JVM binders but leaves the registry for its owner to close.
 */
public final class MetricsContextSnapshot implements AutoCloseable {
    private final MeterRegistry meterRegistry;
    private final @Nullable JvmGcMetrics jvmGcMetrics;

    public MetricsContextSnapshot(
            MetricsContext config,
            String pipelinePath,
            String clusterUrl
    ) {
        this.meterRegistry = config.getRegistry();
        // Apply common tags.
        meterRegistry.config().commonTags(
                "pipeline_path", pipelinePath,
                "pipeline_cluster", clusterUrl
        );
        if (config.isJvmMetricsEnabled()) {
            this.jvmGcMetrics = new JvmGcMetrics();
            jvmGcMetrics.bindTo(meterRegistry);
            new ClassLoaderMetrics().bindTo(meterRegistry);
            new JvmMemoryMetrics().bindTo(meterRegistry);
            new ProcessorMetrics().bindTo(meterRegistry);
            new JvmThreadMetrics().bindTo(meterRegistry);
            new JvmThreadDeadlockMetrics().bindTo(meterRegistry);
        } else {
            this.jvmGcMetrics = null;
        }
    }

    public MeterRegistry getRegistry() {
        return meterRegistry;
    }

    @Override
    public void close() {
        // Release only the bindings we created.
        if (jvmGcMetrics != null) {
            jvmGcMetrics.close();
        }
    }
}
