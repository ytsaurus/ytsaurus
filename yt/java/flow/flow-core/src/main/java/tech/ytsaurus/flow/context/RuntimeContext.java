package tech.ytsaurus.flow.context;

import java.util.Map;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.resource.FlowResource;
import tech.ytsaurus.flow.row.MessageBuilder;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * An interface for runtime context accessible inside {@link tech.ytsaurus.flow.function.FlowFunction}.
 */
public interface RuntimeContext extends StatefulContext {

    /**
     * Creates a new message builder for the specified stream.
     *
     * @param streamId The identifier of the stream for which to create the message builder.
     * @return A new instance of {@link MessageBuilder} configured for the given stream.
     * @throws IllegalArgumentException If streamId is not registered for computation.
     */
    MessageBuilder createMessageBuilder(String streamId);


    /**
     * Min Watermark value for all timer streams.
     *
     * <p>Returns {@code 0} when the computation carries no watermarks
     * (SwiftMap / SwiftOrderedSource computations), meaning no event time has advanced.
     *
     * @return Min watermark timestamp.
     */
    Long getEpochInputEventWatermark();

    /**
     * Watermark value for the specified stream.
     *
     * @param streamId The identifier of the stream.
     * @return Watermark timestamp, or {@code null} if the stream carries no watermark.
     */
    @Nullable Long getEpochEventWatermark(String streamId);

    /**
     * Returns content of the "parameters" section from computation static spec.
     *
     * @return Map of parameter name to YTree parameter value.
     */
    Map<String, YTreeNode> getComputationParameters();

    /**
     * Returns content of the "parameters" section from computation dynamic spec.
     *
     * @return Map of dynamic parameter name to YTree parameter value.
     */
    Map<String, YTreeNode> getComputationDynamicParameters();

    /**
     * Returns StreamSpecs instance for the current Computation.
     *
     * @return Stream specs.
     */
    StreamSpecs getStreamSpecs();

    /**
     * Returns a companion-hosted resource by the alias from the computation's
     * {@code required_resource_ids} entry (the resource id when no alias is set).
     *
     * <p>Read it here on every call: the instance is served per batch, and one cached in the
     * computation across batches keeps being used after the worker retired it and its unload
     * hook ran.
     *
     * @param alias The alias of the required resource.
     * @return The initialized resource instance.
     * @throws IllegalStateException If no resource with such alias is available; companion-hosted
     *                               resources must be listed in the computation's
     *                               {@code required_resource_ids}.
     */
    default FlowResource getResource(String alias) {
        throw new UnsupportedOperationException("This RuntimeContext does not host companion resources");
    }

    /**
     * Companion-hosted resource by alias, checked against the expected type.
     *
     * @param alias The alias of the required resource.
     * @param type  The expected resource type.
     * @param <T>   The expected resource type.
     * @return The initialized resource instance.
     * @throws IllegalStateException If no resource with such alias is available, or the bound
     *                               resource is not of the expected type.
     */
    default <T extends FlowResource> T getResource(String alias, Class<T> type) {
        FlowResource resource = getResource(alias);
        if (!type.isInstance(resource)) {
            throw new IllegalStateException(
                    "Companion resource '%s' is a %s, not the expected %s".formatted(
                            alias, resource.getClass().getName(), type.getName()));
        }
        return type.cast(resource);
    }
}
