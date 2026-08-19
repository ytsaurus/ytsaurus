package tech.ytsaurus.flow.testutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptor;

public class TestDoProcessRequest {
    private final String computationId;
    private final List<ExtendedMessage> messages;
    private final List<Timer> timers;
    private final Map<String, Map<Payload, ExternalState>> externalStates;
    private final Map<String, Map<Payload, ExternalState>> joinedExternalStates;
    private final Map<String, Map<Payload, InternalState>> internalStates;
    private final Map<String, Long> watermarks;

    TestDoProcessRequest(
            String computationId,
            List<ExtendedMessage> messages,
            List<Timer> timers,
            Map<String, Map<Payload, ExternalState>> externalStates,
            Map<String, Map<Payload, ExternalState>> joinedExternalStates,
            Map<String, Map<Payload, InternalState>> internalStates,
            Map<String, Long> watermarks
    ) {
        this.computationId = computationId;
        this.messages = messages;
        this.timers = timers;
        this.externalStates = externalStates;
        this.joinedExternalStates = joinedExternalStates;
        this.internalStates = internalStates;
        this.watermarks = watermarks;
    }

    public static Builder builder(String computationId) {
        return new Builder().setComputationId(computationId);
    }

    public String getComputationId() {
        return computationId;
    }

    public List<ExtendedMessage> getMessages() {
        return messages;
    }

    public List<Timer> getTimers() {
        return timers;
    }

    public Map<String, Map<Payload, ExternalState>> getExternalStates() {
        return externalStates;
    }

    public Map<String, Map<Payload, ExternalState>> getJoinedExternalStates() {
        return joinedExternalStates;
    }

    public Map<String, Map<Payload, InternalState>> getInternalStates() {
        return internalStates;
    }

    public Map<String, Long> getWatermarks() {
        return watermarks;
    }

    public static class Builder {
        private @Nullable String computationId;
        private List<ExtendedMessage> messages = new ArrayList<>();
        private List<Timer> timers = new ArrayList<>();
        private final Map<String, Map<Payload, ExternalState>> externalStates = new HashMap<>();
        private final Map<String, Map<Payload, ExternalState>> joinedExternalStates = new HashMap<>();
        private final Map<String, Map<Payload, InternalState>> internalStates = new HashMap<>();
        private Map<String, Long> watermarks = Collections.emptyMap();

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

        /**
         * Seeds the mutable state for {@code descriptor} at {@code key} with {@code value}.
         *
         * @param <T> state value type.
         */
        public <T> Builder setState(
                StateDescriptor<T> descriptor,
                Payload key,
                T value
        ) {
            applyStateMutation(descriptor, key, acc -> acc.set(value));
            return this;
        }

        /**
         * Seeds a read-only joined external state for {@code descriptor} at {@code key}.
         */
        public Builder setState(
                JoinedExternalStateDescriptor descriptor,
                Payload key,
                Payload value
        ) {
            joinedExternalStates
                    .computeIfAbsent(descriptor.getName(), name -> new HashMap<>())
                    .put(key, new ExternalState(value));
            return this;
        }

        /**
         * Seeds a cleared (reset) state for {@code descriptor} at {@code key}, as a {@code clear()}
         * would.
         *
         * @param <T> state value type.
         */
        public <T> Builder clearState(
                StateDescriptor<T> descriptor,
                Payload key
        ) {
            applyStateMutation(descriptor, key, StateAccessor::clear);
            return this;
        }

        /**
         * Seeds the request maps with the raw state captured for {@code op} (a {@code set} or
         * {@code clear}), routing it to the internal or external map per its kind.
         */
        private <T> void applyStateMutation(
                StateDescriptor<T> descriptor,
                Payload key,
                Consumer<StateAccessor<T>> op
        ) {
            var seed = StateSeeder.capture(descriptor, key, op);
            String name = descriptor.getName();
            switch (seed.kind()) {
                case INTERNAL -> internalStates
                        .computeIfAbsent(name, n -> new HashMap<>())
                        .put(key, (InternalState) seed.state());
                case EXTERNAL -> externalStates
                        .computeIfAbsent(name, n -> new HashMap<>())
                        .put(key, (ExternalState) seed.state());
            }
        }

        public Builder setWatermarks(Map<String, Long> watermarks) {
            this.watermarks = watermarks;
            return this;
        }

        public Builder addWatermark(String streamId, long watermark) {
            if (!(this.watermarks instanceof HashMap)) {
                this.watermarks = new HashMap<>(this.watermarks);
            }
            this.watermarks.put(streamId, watermark);
            return this;
        }

        public TestDoProcessRequest build() {
            return new TestDoProcessRequest(
                    computationId, messages, timers, externalStates, joinedExternalStates, internalStates, watermarks);
        }
    }
}
