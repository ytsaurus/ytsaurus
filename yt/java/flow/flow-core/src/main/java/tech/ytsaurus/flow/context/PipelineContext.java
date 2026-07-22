package tech.ytsaurus.flow.context;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.stream.FlowStream;

/**
 * Build-phase configuration container for a Flow pipeline.
 *
 * <p>{@link PipelineContext} is a mutable builder for the set of {@link Computation},
 * {@link FlowStream}, and the {@link MetricsContext} that make up a pipeline. It is
 * intended to be populated once at application startup, before any worker thread observes it.
 *
 * <p><b>Thread-safety.</b> This class is <em>not</em> thread-safe. All registration calls
 * must happen on the configuring thread (or be externally synchronized).
 *
 * <p>Example usage:
 * <pre>{@code
 * PipelineContext context = new PipelineContext();
 * context.registerComputation(new MyComputation("comp-1"));
 * context.registerStream(FlowStreams.typed("stream-1", MyMessage.class));
 * }</pre>
 */
public class PipelineContext {
    private static final Logger log = LoggerFactory.getLogger(PipelineContext.class);

    private final Map<String, Computation> computations = new HashMap<>();
    private final Map<String, FlowStream<?>> streams = new HashMap<>();
    private MetricsContext metricsContext = MetricsContext.builder().build();

    /**
     * Creates an empty pipeline context.
     *
     * <p>Computations and streams can be registered later using
     * {@link #registerComputation(Computation)} and {@link #registerStream(FlowStream)}.
     */
    public PipelineContext() {
    }

    /**
     * Creates a pipeline context and registers the provided computations.
     *
     * @param computations the computations to register in this context
     * @throws IllegalArgumentException if any computation ID is duplicated
     */
    public PipelineContext(Iterable<Computation> computations) {
        for (Computation computation : computations) {
            registerComputation(computation);
            log.info("Registered computation: {}", computation.getComputationId());
        }
    }

    /**
     * Creates a pipeline context, registers the provided computations, and binds the supplied
     * {@link MetricsContext}.
     *
     * @param computations   the computations to register in this context
     * @param metricsContext the metrics context to bind
     * @throws IllegalArgumentException if any computation ID is duplicated
     */
    public PipelineContext(Iterable<Computation> computations, MetricsContext metricsContext) {
        this(computations);
        this.metricsContext = metricsContext;
    }

    /**
     * Registers a computation in the context.
     *
     * @param computation the computation to be registered
     * @throws IllegalArgumentException if a computation with the same id is already registered
     */
    public void registerComputation(Computation computation) {
        var existing = computations.putIfAbsent(computation.getComputationId(), computation);
        if (existing != null) {
            throw new IllegalArgumentException(
                    "Computation %s already exists".formatted(computation.getComputationId())
            );
        }
    }

    /**
     * Registers a stream in the context.
     *
     * <p>Important! Each stream must have a corresponding stream definition in the
     * computation static spec with a compatible schema.
     *
     * @param stream the stream to be registered
     * @throws IllegalArgumentException if a stream with the same ID is already registered
     */
    public void registerStream(FlowStream<?> stream) {
        if (streams.put(stream.getStreamId(), stream) != null) {
            throw new IllegalArgumentException(
                    "Stream %s already exists".formatted(stream.getStreamId())
            );
        }
    }

    /**
     * Registers all streams from the given iterable in the context.
     *
     * <p>Each stream is registered via {@link #registerStream(FlowStream)}, so the same
     * duplicate-id semantics apply.
     *
     * @param streams the streams to be registered
     * @throws IllegalArgumentException if a stream with an already registered id is encountered
     */
    public void registerStreams(Iterable<FlowStream<?>> streams) {
        for (FlowStream<?> stream : streams) {
            registerStream(stream);
        }
    }

    /**
     * Registers a stream in the context if no stream with the same id is already present.
     *
     * <p>Unlike {@link #registerStream(FlowStream)}, this method silently returns {@code false}
     * when a duplicate is encountered instead of throwing.
     *
     * @param stream the stream to be registered
     * @return {@code true} if the stream was added, {@code false} if a stream with the same id
     * was already present
     */
    public boolean registerStreamIfAbsent(FlowStream<?> stream) {
        return streams.putIfAbsent(stream.getStreamId(), stream) == null;
    }

    /**
     * Registers a {@link MetricsContext} for this pipeline, replacing the default context.
     *
     * <p>Typically called before the companion server is started so that subsequent metric
     * registrations are routed to the supplied {@link io.micrometer.core.instrument.MeterRegistry}.
     *
     * @param metricsContext the metrics context to use.
     */
    public void registerMetricsContext(MetricsContext metricsContext) {
        this.metricsContext = metricsContext;
    }

    /**
     * Returns the {@link MetricsContext} associated with this pipeline.
     *
     * <p>If no context has been registered explicitly, a default context built from
     * {@link MetricsContext.Builder} defaults (JVM metrics enabled) is returned.
     *
     * @return the metrics context (never {@code null}).
     */
    public MetricsContext getMetricsContext() {
        return metricsContext;
    }

    /**
     * Returns the computations registered in this context.
     *
     * @return the live map of registered computations keyed by computation id
     */
    Map<String, Computation> getComputations() {
        return computations;
    }

    /**
     * Returns the streams registered in this context.
     *
     * @return the live map of registered streams keyed by stream id
     */
    Map<String, FlowStream<?>> getStreams() {
        return streams;
    }
}
