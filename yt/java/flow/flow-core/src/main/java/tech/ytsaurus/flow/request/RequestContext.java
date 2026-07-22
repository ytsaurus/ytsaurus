package tech.ytsaurus.flow.request;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.Visit;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * RequestContext class contains full context required for request processing.
 */
public class RequestContext implements YTreeConvertible {
    private static final Logger log = LoggerFactory.getLogger(RequestContext.class);
    private final GUID jobId;
    private final GUID requestId;
    private final String computationId;
    private final List<ExtendedMessage> messages;
    private final List<Timer> timers;
    private final List<Visit> visits;
    private final StreamSpecs streamSpecs;
    private final Map<String, StatesHolder<InternalState>> internalStates;
    private final Map<String, StatesHolder<ExternalState>> externalStates;
    private final Map<String, StatesHolder<ExternalState>> joinedExternalStates;
    private final Map<String, Long> watermarks;
    private final Long minWatermark;
    private final Job job;

    RequestContext(Builder builder) {
        this.jobId = Objects.requireNonNull(builder.jobId);
        this.requestId = Objects.requireNonNull(builder.requestId);
        this.computationId = Objects.requireNonNull(builder.computationId);
        this.messages = builder.messages;
        this.timers = builder.timers;
        this.visits = builder.visits;
        this.internalStates = builder.internalStates;
        this.externalStates = builder.externalStates;
        this.joinedExternalStates = builder.joinedExternalStates;
        this.watermarks = builder.watermarks;
        this.minWatermark = builder.minWatermark;
        this.job = builder.job;
        if (builder.streamSpecsOverride != null) {
            // User overrides from input request.
            this.streamSpecs = builder.streamSpecsOverride;
        } else {
            this.streamSpecs = Objects.requireNonNull(job).getStreamSpecs();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public GUID getJobId() {
        return jobId;
    }

    public GUID getRequestId() {
        return requestId;
    }

    public String getComputationId() {
        return computationId;
    }

    public Job getJob() {
        return job;
    }

    public List<ExtendedMessage> getMessages() {
        return messages;
    }

    public StreamSpecs getStreamSpecs() {
        return streamSpecs;
    }

    public List<Timer> getTimers() {
        return timers;
    }

    public List<Visit> getVisits() {
        return visits;
    }

    public Map<String, StatesHolder<InternalState>> getInternalStates() {
        return internalStates;
    }

    public Map<String, StatesHolder<ExternalState>> getExternalStates() {
        return externalStates;
    }

    /**
     * Read-only external state joined from another computation. Never written back.
     */
    public Map<String, StatesHolder<ExternalState>> getJoinedExternalStates() {
        return joinedExternalStates;
    }

    public Map<String, Long> getWatermarks() {
        return watermarks;
    }

    public Long getMinWatermark() {
        return minWatermark;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        YTreeBuilder builder = YTree.builder().beginMap();
        builder.key("job_id").value(jobId.toString())
                .key("request_id").value(requestId.toString())
                .key("computation_id").value(computationId)
                .key("job").value(job.toYTree());
        builder.key("messages").beginList();
        for (var message : messages) {
            builder.value(message.toYTree());
        }
        builder.endList();
        builder.key("timers").beginList();
        for (var timer : timers) {
            builder.value(timer.toYTree());
        }
        builder.endList();
        builder.key("visits").beginList();
        for (var visit : visits) {
            builder.value(visit.toYTree());
        }
        builder.endList();
        builder.key("stream_specs").value(streamSpecs.toYTree());
        builder.key("internal_states").beginMap();
        for (var entry : internalStates.entrySet()) {
            builder.key(entry.getKey()).value(entry.getValue().toYTree());
        }
        builder.endMap();
        builder.key("external_states").beginMap();
        for (var entry : externalStates.entrySet()) {
            builder.key(entry.getKey()).value(entry.getValue().toYTree());
        }
        builder.endMap();
        builder.key("joined_external_states").beginMap();
        for (var entry : joinedExternalStates.entrySet()) {
            builder.key(entry.getKey()).value(entry.getValue().toYTree());
        }
        builder.endMap();
        builder.key("min_watermark").value(minWatermark);
        builder.key("watermarks").beginMap();
        for (var entry : watermarks.entrySet()) {
            builder.key(entry.getKey()).value(entry.getValue());
        }
        builder.endMap();
        return builder.endMap().build();
    }

    public static class Builder {
        private @Nullable GUID jobId;
        private @Nullable GUID requestId;
        private @Nullable String computationId;
        private List<ExtendedMessage> messages = Collections.emptyList();
        private List<Timer> timers = Collections.emptyList();
        private List<Visit> visits = Collections.emptyList();
        private Map<String, Long> watermarks = Collections.emptyMap();
        private @Nullable Long minWatermark;
        private @Nullable StreamSpecs streamSpecsOverride;
        private Map<String, StatesHolder<InternalState>> internalStates = Collections.emptyMap();
        private Map<String, StatesHolder<ExternalState>> externalStates = Collections.emptyMap();
        private Map<String, StatesHolder<ExternalState>> joinedExternalStates = Collections.emptyMap();
        private @Nullable Job job;

        Builder() {
        }

        public Builder setJobId(GUID jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setRequestId(GUID requestId) {
            this.requestId = requestId;
            return this;
        }

        public Builder setComputationId(String computationId) {
            this.computationId = computationId;
            return this;
        }

        public Builder setMessages(List<ExtendedMessage> messages) {
            this.messages = messages;
            return this;
        }

        public Builder setTimers(List<Timer> timers) {
            this.timers = timers;
            return this;
        }

        public Builder setVisits(@Nullable List<Visit> visits) {
            this.visits = visits == null ? Collections.emptyList() : visits;
            return this;
        }

        public Builder setStreamSpecsOverride(StreamSpecs streamSpecsOverride) {
            this.streamSpecsOverride = streamSpecsOverride;
            return this;
        }

        public Builder setInternalStates(Map<String, StatesHolder<InternalState>> internalStates) {
            this.internalStates = internalStates;
            return this;
        }

        public Builder setExternalStates(Map<String, StatesHolder<ExternalState>> externalStates) {
            this.externalStates = externalStates;
            return this;
        }

        public Builder setJoinedExternalStates(Map<String, StatesHolder<ExternalState>> joinedExternalStates) {
            this.joinedExternalStates = joinedExternalStates;
            return this;
        }

        public Builder setWatermarks(Map<String, Long> watermarks) {
            this.watermarks = watermarks;
            return this;
        }

        public Builder setMinWatermark(Long minWatermark) {
            this.minWatermark = minWatermark;
            return this;
        }

        public Builder setJob(Job job) {
            this.job = job;
            return this;
        }

        public RequestContext build() {
            return new RequestContext(this);
        }
    }

}
