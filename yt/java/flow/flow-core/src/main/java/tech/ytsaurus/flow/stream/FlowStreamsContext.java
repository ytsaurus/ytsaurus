package tech.ytsaurus.flow.stream;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Immutable registry for {@link FlowStream} instances.
 *
 * <p>Instances are safe to publish to multiple threads without additional synchronization.
 * The map is captured at construction time; subsequent changes to the source map are not
 * reflected here.
 */
public final class FlowStreamsContext {
    private final Map<String, FlowStream<?>> streams;

    /**
     * Creates a stream context with a defensive copy of the supplied streams.
     *
     * @param streams the streams to expose; insertion order is preserved
     */
    public FlowStreamsContext(Map<String, FlowStream<?>> streams) {
        this.streams = Map.copyOf(streams);
    }

    /**
     * Retrieves a stream by its ID.
     *
     * @param streamId the unique identifier of the stream to retrieve
     * @return the stream associated with the given ID, or {@code null} if not found
     */
    public @Nullable FlowStream<?> getStream(String streamId) {
        return streams.get(streamId);
    }
}
