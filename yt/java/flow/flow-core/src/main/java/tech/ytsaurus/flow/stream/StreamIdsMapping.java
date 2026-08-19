package tech.ytsaurus.flow.stream;

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

public class StreamIdsMapping {
    private final Map<Long, String> streamSpecIdToStreamId;
    private final Map<String, Long> streamIdToStreamSpecId;

    StreamIdsMapping(
            Map<Long, String> streamSpecIdToStreamId,
            Map<String, Long> streamIdToStreamSpecId
    ) {
        this.streamSpecIdToStreamId = Map.copyOf(streamSpecIdToStreamId);
        this.streamIdToStreamSpecId = Map.copyOf(streamIdToStreamSpecId);
    }

    public static Builder builder() {
        return new Builder();
    }

    public @Nullable Long getStreamSpecId(String streamId) {
        return streamIdToStreamSpecId.get(streamId);
    }

    public @Nullable String getStreamId(Long streamSpecId) {
        return streamSpecIdToStreamId.get(streamSpecId);
    }

    public static class Builder {
        private final Map<Long, String> streamSpecIdToStreamId = new HashMap<>();
        private final Map<String, Long> streamIdToStreamSpecId = new HashMap<>();

        public Builder addMapping(String streamId, Long streamSpecId) {
            streamSpecIdToStreamId.put(streamSpecId, streamId);
            streamIdToStreamSpecId.put(streamId, streamSpecId);
            return this;
        }

        public StreamIdsMapping build() {
            return new StreamIdsMapping(streamSpecIdToStreamId, streamIdToStreamSpecId);
        }
    }
}
