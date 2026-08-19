package tech.ytsaurus.flow.stream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Provides access to Streams by streamId and streamSpecId.
 */
public class StreamSpecs implements YTreeConvertible {
    private final Map<String, FlowStream<?>> streamIdToStream;
    private final StreamIdsMapping mapping;

    public StreamSpecs(StreamIdsMapping mapping, List<? extends FlowStream<?>> streams) {
        this.mapping = mapping;
        this.streamIdToStream = new HashMap<>();
        for (var stream : streams) {
            streamIdToStream.put(stream.getStreamId(), stream);
        }
    }

    /**
     * Returns stream by streamId.
     *
     * @param streamId String streamId.
     * @return Stream, or {@code null} if not found.
     */
    public @Nullable FlowStream<?> getStream(String streamId) {
        return streamIdToStream.get(streamId);
    }

    /**
     * Returns stream by streamSpecId.
     *
     * @param streamSpecId Long streamSpecId.
     * @return Stream, or {@code null} if not found.
     */
    public @Nullable FlowStream<?> getStream(Long streamSpecId) {
        return streamIdToStream.get(mapping.getStreamId(streamSpecId));
    }

    public String getStreamId(Long streamSpecId) {
        return Objects.requireNonNull(mapping.getStreamId(streamSpecId));
    }

    public long getStreamSpecId(String streamId) {
        return Objects.requireNonNull(mapping.getStreamSpecId(streamId));
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        var builder = YTree.builder().beginMap();
        for (var stream : streamIdToStream.values()) {
            builder.key(stream.getStreamId()).value(stream.toYTree());
        }
        return builder.endMap().build();
    }
}
