package tech.ytsaurus.flow.testutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor;
import tech.ytsaurus.flow.state.ProtoExternalStateDescriptor;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.testutils.StateSeeder.CapturedSeed;

public class TestDoProcessRequest {
    private final String computationId;
    private final List<ExtendedMessage> messages;
    private final List<Timer> timers;
    private final Map<String, Map<Payload, State>> externalStates;
    private final Map<String, Map<Payload, State>> joinedExternalStates;
    private final Map<String, Map<Payload, State>> internalStates;
    private final Map<String, String> protoStateTypes;
    private final List<Consumer<Map<String, TableSchema>>> stateSeeds;
    private final Map<String, Long> watermarks;

    TestDoProcessRequest(
            String computationId,
            List<ExtendedMessage> messages,
            List<Timer> timers,
            Map<String, Map<Payload, State>> externalStates,
            Map<String, Map<Payload, State>> joinedExternalStates,
            Map<String, Map<Payload, State>> internalStates,
            Map<String, String> protoStateTypes,
            List<Consumer<Map<String, TableSchema>>> stateSeeds,
            Map<String, Long> watermarks
    ) {
        this.computationId = computationId;
        this.messages = messages;
        this.timers = timers;
        this.externalStates = externalStates;
        this.joinedExternalStates = joinedExternalStates;
        this.internalStates = internalStates;
        this.protoStateTypes = protoStateTypes;
        this.stateSeeds = stateSeeds;
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

    public Map<String, Map<Payload, State>> getExternalStates() {
        return externalStates;
    }

    public Map<String, Map<Payload, State>> getJoinedExternalStates() {
        return joinedExternalStates;
    }

    public Map<String, Map<Payload, State>> getInternalStates() {
        return internalStates;
    }

    /**
     * Proto message type per external state seeded through a proto descriptor; such states are
     * sent in the proto wire format.
     */
    public Map<String, String> getProtoStateTypes() {
        return protoStateTypes;
    }

    public Map<String, Long> getWatermarks() {
        return watermarks;
    }

    /**
     * Runs the seeded state mutations against {@code externalStateSchemas}, the schemas declared
     * on the harness, filling {@link #getInternalStates} and {@link #getExternalStates}. Seeding
     * waits for this call because the schemas belong to the harness, not to the request.
     */
    void seedStates(Map<String, TableSchema> externalStateSchemas) {
        for (var seed : stateSeeds) {
            seed.accept(externalStateSchemas);
        }
    }

    public static class Builder {
        private @Nullable String computationId;
        private List<ExtendedMessage> messages = new ArrayList<>();
        private List<Timer> timers = new ArrayList<>();
        private final Map<String, Map<Payload, State>> externalStates = new HashMap<>();
        private final Map<String, Map<Payload, State>> joinedExternalStates = new HashMap<>();
        private final Map<String, Map<Payload, State>> internalStates = new HashMap<>();
        private final Map<String, String> protoStateTypes = new HashMap<>();
        private final Set<String> rowExternalStates = new HashSet<>();
        private final List<Consumer<Map<String, TableSchema>>> stateSeeds = new ArrayList<>();
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
                    .put(key, new State(
                            CodecRegistry.getInstance().getPayloadCodec()
                                    .codecFor(value.getSchema()).encode(value)));
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
         * Defers {@code op} (a {@code set} or {@code clear}) to {@link #seedStates}, when the
         * state's schema is known.
         */
        private <T> void applyStateMutation(
                StateDescriptor<T> descriptor,
                Payload key,
                Consumer<StateAccessor<T>> op
        ) {
            stateSeeds.add(schemas -> seedState(descriptor, key, op, schemas.get(descriptor.getName())));
        }

        /**
         * Seeds the request maps with the raw state captured for {@code op}, routing it to the
         * internal or external map per its kind.
         */
        private <T> void seedState(
                StateDescriptor<T> descriptor,
                Payload key,
                Consumer<StateAccessor<T>> op,
                @Nullable TableSchema stateSchema
        ) {
            String name = descriptor.getName();
            if (descriptor instanceof ProtoExternalStateDescriptor<?>) {
                requireUnmixedFormat(name, rowExternalStates);
            } else if (descriptor instanceof ExternalStateDescriptor) {
                requireUnmixedFormat(name, protoStateTypes.keySet());
            }
            var seed = StateSeeder.capture(descriptor, key, op, stateSchema);
            switch (seed.kind()) {
                case INTERNAL -> internalStates
                        .computeIfAbsent(name, n -> new HashMap<>())
                        .put(key, seed.state());
                case EXTERNAL -> externalStates
                        .computeIfAbsent(name, n -> new HashMap<>())
                        .put(key, seed.state());
            }
            if (descriptor instanceof ProtoExternalStateDescriptor<?> protoDescriptor) {
                protoStateTypes.put(name, StateSeeder.protoTypeOf(protoDescriptor));
            } else if (seed.kind() == CapturedSeed.Kind.EXTERNAL) {
                rowExternalStates.add(name);
            }
        }

        /**
         * A state travels in one wire format, so its seeds must all be proto or all be rows.
         */
        private static void requireUnmixedFormat(String name, Set<String> otherFormat) {
            if (otherFormat.contains(name)) {
                throw new IllegalArgumentException(
                        "External state %s mixes proto-format and row-format seeds".formatted(name));
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
                    computationId,
                    messages,
                    timers,
                    externalStates,
                    joinedExternalStates,
                    internalStates,
                    protoStateTypes,
                    stateSeeds,
                    watermarks);
        }
    }
}
