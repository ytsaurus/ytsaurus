package tech.ytsaurus.flow.context;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.row.Keyed;
import tech.ytsaurus.flow.row.MessageBuilder;
import tech.ytsaurus.flow.state.DefaultStateManager;
import tech.ytsaurus.flow.state.ExternalStateAccessor;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor;
import tech.ytsaurus.flow.state.JoinedProtoExternalStateDescriptor;
import tech.ytsaurus.flow.state.ProtoExternalStateDescriptor;
import tech.ytsaurus.flow.state.ProtoStateAccessor;
import tech.ytsaurus.flow.state.ReadOnlyExternalStateAccessor;
import tech.ytsaurus.flow.state.ReadOnlyProtoStateAccessor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateBackend;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.state.StateManager;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Default implementation of {@link RuntimeContext}.
 */
public class DefaultRuntimeContext implements RuntimeContext {
    private final StateManager stateManager;
    private final StreamSpecs streamSpecs;
    private final Map<String, Long> watermarks;
    private final Map<String, Long> systemWatermarks;
    private final Long minWatermark;
    private final Map<String, YTreeNode> computationParameters;
    private final Map<String, YTreeNode> computationDynamicParameters;
    private final Map<String, FlowResource> resources;

    private DefaultRuntimeContext(Builder builder) {
        this.stateManager = new DefaultStateManager(Objects.requireNonNull(builder.stateBackend, "stateBackend"));
        this.streamSpecs = builder.streamSpecs;
        this.watermarks = builder.watermarks;
        this.systemWatermarks = builder.systemWatermarks;
        this.minWatermark = builder.minWatermark;
        this.computationParameters = builder.computationParameters;
        this.computationDynamicParameters = builder.computationDynamicParameters;
        this.resources = builder.resources;
    }

    /**
     * Creates a builder. Only the state backend is required; the minimum watermark defaults to
     * {@code 0} and every other value to empty.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @see RuntimeContext#createMessageBuilder(String)
     */
    @Override
    public MessageBuilder createMessageBuilder(String streamId) {
        var stream = streamSpecs.getStream(streamId);
        if (stream == null) {
            throw new IllegalArgumentException("Unknown streamId: %s".formatted(streamId));
        }
        return new MessageBuilder(streamId, stream.getSchema());
    }

    /**
     * Retrieves or creates a state accessor for the given descriptor and key.
     *
     * @param descriptor The state descriptor used to create the state accessor.
     * @param key        The key associated with the state.
     * @return The state accessor created by the descriptor for the given key.
     */
    @Override
    public <T> StateAccessor<T> getState(
            StateDescriptor<T> descriptor,
            Keyed key
    ) {
        return stateManager.create(descriptor, key);
    }

    /**
     * Retrieves external state accessor for the given descriptor and key.
     *
     * @param descriptor The external state descriptor used to create the state accessor.
     * @param key        The key associated with the state.
     * @return The state accessor created by the descriptor for the given key.
     */
    @Override
    public ExternalStateAccessor getState(
            ExternalStateDescriptor descriptor,
            Keyed key
    ) {
        return (ExternalStateAccessor) stateManager.create(descriptor, key);
    }

    /**
     * Retrieves a read-only joined external state accessor for the given descriptor and key.
     *
     * @param descriptor The joined external state descriptor used to create the accessor.
     * @param key        The key associated with the state.
     * @return The read-only accessor created by the descriptor for the given key.
     */
    @Override
    public ReadOnlyExternalStateAccessor getState(
            JoinedExternalStateDescriptor descriptor,
            Keyed key
    ) {
        return (ReadOnlyExternalStateAccessor) stateManager.create(descriptor, key);
    }

    /**
     * Retrieves a proto-format external state accessor for the given descriptor and key.
     *
     * @param descriptor The proto external state descriptor used to create the accessor.
     * @param key        The key associated with the state.
     * @return The accessor created by the descriptor for the given key.
     */
    @Override
    public <T extends Message> ProtoStateAccessor<T> getState(
            ProtoExternalStateDescriptor<T> descriptor,
            Keyed key
    ) {
        return (ProtoStateAccessor<T>) stateManager.create(descriptor, key);
    }

    /**
     * Retrieves a read-only proto-format joined external state accessor for the given
     * descriptor and key.
     *
     * @param descriptor The joined proto external state descriptor used to create the accessor.
     * @param key        The key associated with the state.
     * @return The read-only accessor created by the descriptor for the given key.
     */
    @Override
    public <T extends Message> ReadOnlyProtoStateAccessor<T> getState(
            JoinedProtoExternalStateDescriptor<T> descriptor,
            Keyed key
    ) {
        return (ReadOnlyProtoStateAccessor<T>) stateManager.create(descriptor, key);
    }

    /**
     * @see RuntimeContext#getEpochInputEventWatermark()
     */
    @Override
    public Long getEpochInputEventWatermark() {
        return minWatermark;
    }

    /**
     * @see RuntimeContext#getEpochEventWatermark(String)
     */
    @Override
    public @Nullable Long getEpochEventWatermark(String streamId) {
        return watermarks.get(streamId);
    }

    /**
     * @see RuntimeContext#getEpochSystemWatermark(String)
     */
    @Override
    public @Nullable Long getEpochSystemWatermark(String streamId) {
        return systemWatermarks.get(streamId);
    }

    @Override
    public Map<String, YTreeNode> getComputationParameters() {
        return computationParameters;
    }

    @Override
    public Map<String, YTreeNode> getComputationDynamicParameters() {
        return computationDynamicParameters;
    }

    @Override
    public StreamSpecs getStreamSpecs() {
        return streamSpecs;
    }

    /**
     * @see RuntimeContext#getResource(String)
     */
    @Override
    public FlowResource getResource(String alias) {
        var resource = resources.get(alias);
        if (resource == null) {
            throw new IllegalStateException(
                    ("Companion resource is not available in this process; companion-hosted resources "
                            + "must be listed in the computation's required_resource_ids (Alias: %s)")
                            .formatted(alias)
            );
        }
        return resource;
    }

    /**
     * Builder for {@link DefaultRuntimeContext}.
     */
    public static class Builder {
        private @Nullable StateBackend stateBackend;
        private StreamSpecs streamSpecs = new StreamSpecs(StreamIdsMapping.builder().build(), List.of());
        private Map<String, Long> watermarks = Collections.emptyMap();
        private Map<String, Long> systemWatermarks = Collections.emptyMap();
        private Long minWatermark = 0L;
        private Map<String, YTreeNode> computationParameters = Collections.emptyMap();
        private Map<String, YTreeNode> computationDynamicParameters = Collections.emptyMap();
        private Map<String, FlowResource> resources = Collections.emptyMap();

        Builder() {
        }

        /**
         * Sets the backend holding the batch states. Required.
         */
        public Builder setStateBackend(StateBackend stateBackend) {
            this.stateBackend = stateBackend;
            return this;
        }

        /**
         * Sets the computation streams. Defaults to no streams.
         */
        public Builder setStreamSpecs(StreamSpecs streamSpecs) {
            this.streamSpecs = streamSpecs;
            return this;
        }

        /**
         * Sets the event watermarks keyed by stream id.
         */
        public Builder setWatermarks(Map<String, Long> watermarks) {
            this.watermarks = watermarks;
            return this;
        }

        /**
         * Sets the system watermarks keyed by stream id.
         */
        public Builder setSystemWatermarks(Map<String, Long> systemWatermarks) {
            this.systemWatermarks = systemWatermarks;
            return this;
        }

        /**
         * Sets the minimum input event watermark. Defaults to {@code 0}.
         */
        public Builder setMinWatermark(Long minWatermark) {
            this.minWatermark = minWatermark;
            return this;
        }

        public Builder setComputationParameters(Map<String, YTreeNode> computationParameters) {
            this.computationParameters = computationParameters;
            return this;
        }

        public Builder setComputationDynamicParameters(Map<String, YTreeNode> computationDynamicParameters) {
            this.computationDynamicParameters = computationDynamicParameters;
            return this;
        }

        /**
         * Sets the companion-hosted resources acquired for this batch, keyed by alias.
         */
        public Builder setResources(Map<String, FlowResource> resources) {
            this.resources = resources;
            return this;
        }

        public DefaultRuntimeContext build() {
            return new DefaultRuntimeContext(this);
        }
    }
}
