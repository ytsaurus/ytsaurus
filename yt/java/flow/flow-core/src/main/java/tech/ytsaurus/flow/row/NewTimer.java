package tech.ytsaurus.flow.row;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * A new timer created at the companion side.
 */
public class NewTimer implements YTreeConvertible {
    private final long triggerTimestamp;
    private final long eventTimestamp;
    private final @Nullable String streamId;

    public NewTimer(long triggerTimestamp, long eventTimestamp, @Nullable String streamId) {
        this.triggerTimestamp = triggerTimestamp;
        this.eventTimestamp = eventTimestamp;
        this.streamId = streamId;
    }

    public long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    /**
     * @return Stream ID of the timer; null selects the single timer stream from the spec.
     */
    public @Nullable String getStreamId() {
        return streamId;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        return YTree.builder().beginMap()
                .key("trigger_timestamp").value(triggerTimestamp)
                .key("event_timestamp").value(eventTimestamp)
                .key("stream_id").value(streamId)
                .endMap()
                .build();
    }
}
