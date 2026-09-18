package tech.ytsaurus.flow.context;

import java.util.Collections;
import java.util.Map;

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
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Default implementation of {@link RuntimeContext}.
 */
public class DefaultRuntimeContext implements RuntimeContext {
    private final StateManager stateManager;
    private final StreamSpecs streamSpecs;
    private final Map<String, Long> watermarks;
    private final Long minWatermark;
    private final Map<String, YTreeNode> computationParameters;
    private final Map<String, YTreeNode> computationDynamicParameters;
    private final Map<String, FlowResource> resources;

    public DefaultRuntimeContext(
            StateBackend stateBackend,
            StreamSpecs streamSpecs,
            Map<String, Long> watermarks,
            Long minWatermark,
            Map<String, YTreeNode> computationParameters,
            Map<String, YTreeNode> computationDynamicParameters
    ) {
        this(
                stateBackend,
                streamSpecs,
                watermarks,
                minWatermark,
                computationParameters,
                computationDynamicParameters,
                Collections.emptyMap()
        );
    }

    public DefaultRuntimeContext(
            StateBackend stateBackend,
            StreamSpecs streamSpecs,
            Map<String, Long> watermarks,
            Long minWatermark,
            Map<String, YTreeNode> computationParameters,
            Map<String, YTreeNode> computationDynamicParameters,
            Map<String, FlowResource> resources
    ) {
        this.stateManager = new DefaultStateManager(stateBackend);
        this.streamSpecs = streamSpecs;
        this.watermarks = watermarks;
        this.minWatermark = minWatermark;
        this.computationParameters = computationParameters;
        this.computationDynamicParameters = computationDynamicParameters;
        this.resources = resources;
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
}
