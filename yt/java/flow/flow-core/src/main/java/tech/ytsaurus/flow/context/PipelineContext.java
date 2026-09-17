package tech.ytsaurus.flow.context;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.resource.FlowResourceAnnotations;
import tech.ytsaurus.flow.resource.FlowResourceClass;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreamAnnotations;

/**
 * Build-phase configuration container for a Flow pipeline.
 *
 * <p>{@link PipelineContext} is a mutable builder for the set of {@link Computation},
 * {@link FlowStream}, {@link StateDescriptor} and the {@link MetricsContext} that make up a
 * pipeline. It is intended to be populated once at application startup, before any worker thread
 * observes it.
 *
 * <p><b>Thread-safety.</b> This class is <em>not</em> thread-safe. All registration calls
 * must happen on the configuring thread (or be externally synchronized).
 *
 * <p>Example usage:
 * <pre>{@code
 * PipelineContext context = new PipelineContext();
 * context.registerComputation(new MyComputation("comp-1"));
 * context.registerStream(FlowStreams.typed("stream-1", MyMessage.class));
 * context.registerState(StateDescriptors.externalProto("/profile", MyProfile.class));
 * }</pre>
 */
public class PipelineContext {
    private static final Logger log = LoggerFactory.getLogger(PipelineContext.class);

    // Ordered: registration order reaches the pipeline spec and must be deterministic.
    private final Map<String, Computation> computations = new LinkedHashMap<>();
    private final Map<String, FlowStream<?>> streams = new LinkedHashMap<>();
    // A list, not a map by name: state names are scoped to a computation in the pipeline spec, so
    // the owner of an external state and the computations joining it declare it under one name.
    private final List<StateDescriptor<?>> states = new ArrayList<>();
    private final Map<String, Supplier<? extends FlowResource>> resourceFactories = new LinkedHashMap<>();
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
     * Registers a factory under the implementation type name used by
     * {@code spec.resources.<resourceId>.parameters.companion_resource_class}.
     *
     * <p>This is not a resource id or a computation alias. Multiple resource ids in the pipeline
     * spec may select the same registered type. Explicit names do not depend on Java class names
     * or annotations and remain stable when the implementation is renamed.
     *
     * <p>Registration does not invoke the factory. It must return a fresh instance whenever Flow
     * creates a resource: different resource ids and recreated instances have independent lifecycles.
     * A repeated init of an already initialized instance does not call the factory again.
     *
     * @param resourceClassName the spec-facing implementation type name.
     * @param factory           the factory creating fresh resource instances, not a cached singleton.
     * @throws NullPointerException     if the name or factory is null.
     * @throws IllegalArgumentException if the name is blank or already registered.
     */
    public void registerResourceClass(String resourceClassName, Supplier<? extends FlowResource> factory) {
        Objects.requireNonNull(resourceClassName, "resourceClassName must not be null");
        if (resourceClassName.isBlank()) {
            throw new IllegalArgumentException("Resource class name must not be blank");
        }
        Objects.requireNonNull(factory, "factory must not be null");
        if (resourceFactories.putIfAbsent(resourceClassName, factory) != null) {
            throw new IllegalArgumentException(
                    "Resource class %s already exists".formatted(resourceClassName)
            );
        }
    }

    /**
     * Convenience registration using the optional {@link FlowResourceClass} name, or the fully
     * qualified Java class name when the annotation is absent or empty.
     *
     * @param resourceClass the class from which the implementation type name is derived.
     * @param factory       the factory creating a fresh instance whenever Flow needs one.
     * @param <T>           the resource type.
     * @throws NullPointerException     if the class or factory is null.
     * @throws IllegalArgumentException if the derived name is blank or already registered.
     * @see #registerResourceClass(String, Supplier)
     */
    public <T extends FlowResource> void registerResourceClass(Class<T> resourceClass, Supplier<? extends T> factory) {
        Objects.requireNonNull(resourceClass, "resourceClass must not be null");
        registerResourceClass(FlowResourceAnnotations.resourceClassName(resourceClass), factory);
    }

    /**
     * Removes a companion resource class registration.
     *
     * <p>The inverse of {@link #registerResourceClass(String, Supplier)}, for a caller that
     * registers a factory only long enough to be captured by a {@link PipelineContextSnapshot} —
     * the snapshot copies the registrations, so dropping one afterwards leaves the context as it
     * was found instead of retaining the instances that factory closes over.
     *
     * @param resourceClassName the companion-side resource class name to unregister.
     * @return whether a registration under that name existed.
     */
    public boolean unregisterResourceClass(String resourceClassName) {
        return resourceFactories.remove(resourceClassName) != null;
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
     * Registers typed streams declared by {@link FlowMessage}-annotated message POJOs, one stream per
     * declared id. Duplicate-id semantics follow {@link #registerStream(FlowStream)}.
     *
     * @param messageClasses message POJOs annotated with {@link FlowMessage} (and JPA {@code @Entity})
     * @throws IllegalArgumentException if a class lacks {@link FlowMessage}, declares no stream ids,
     *                                  or produces a stream whose id is already registered
     */
    public void registerTypedStreams(Class<?>... messageClasses) {
        for (Class<?> messageClass : messageClasses) {
            registerStreams(FlowStreamAnnotations.fromAnnotatedClass(messageClass));
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
     * Declares a state of the pipeline. Registration is what lets the runner describe the state to
     * the worker: the descriptor source of a profile state is filled from the declared message.
     *
     * <p>State names are scoped to a computation in the pipeline spec, so several descriptors may
     * share a name — the owner of an external state and the computations joining it declare it
     * under one name — as long as they share a state type; states of different types need
     * different names. Registering the same descriptor twice is a no-op.
     *
     * @param state the state descriptor to declare
     * @throws IllegalArgumentException if a state of the same name and another type is declared
     */
    public void registerState(StateDescriptor<?> state) {
        Objects.requireNonNull(state, "state");
        for (StateDescriptor<?> existing : states) {
            if (existing == state) {
                return;
            }
            if (existing.getName().equals(state.getName()) && existing.getStateClass() != state.getStateClass()) {
                throw new IllegalArgumentException(
                        ("State %s is declared with the types %s and %s; states of one name share a type,"
                                + " so give them different names")
                                .formatted(
                                        state.getName(), existing.getStateClass().getName(),
                                        state.getStateClass().getName()));
            }
        }
        states.add(state);
    }

    /**
     * Declares all states from the given iterable, via {@link #registerState(StateDescriptor)}.
     *
     * @param states the states to declare
     * @throws IllegalArgumentException if a state of the same name and another type is declared
     */
    public void registerStates(Iterable<? extends StateDescriptor<?>> states) {
        for (StateDescriptor<?> state : states) {
            registerState(state);
        }
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

    /**
     * Returns the states declared in this context.
     *
     * @return the live list of declared states, in registration order
     */
    List<StateDescriptor<?>> getStates() {
        return states;
    }

    /**
     * Returns the companion resource factories registered in this context.
     *
     * @return the live map of registered resource factories keyed by resource class name
     */
    Map<String, Supplier<? extends FlowResource>> getResourceFactories() {
        return resourceFactories;
    }
}
