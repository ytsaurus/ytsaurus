package tech.ytsaurus.flow.state;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.persistence.Entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.context.DefaultRuntimeContext;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the descriptor-based state API added as part of the state accessor refactoring.
 * See {@code plans/state-descriptor-refactoring.md}.
 */
class StateDescriptorTest {

    private static final String PROTO_STATE = "proto-state";
    private static final String CUSTOM_STATE = "custom-state";
    private static final String RAW_STATE = "raw-state";
    private static final String YSON_STATE = "yson-state";
    private static final String EXT_STATE = "/ext-state";
    private static final String JOINED_STATE = "/joined-state";

    private TableSchema joinedStateSchema;
    private Payload joinedKey;

    private RuntimeContext ctx;
    private ExtendedMessage message;
    private Timer timer;

    @BeforeEach
    void setUp() {
        TableSchema keySchema = TableSchema.builder()
                .addValue("k", TiType.string())
                .build();
        Payload key = new PayloadBuilder(keySchema).set("k", "k1").finish();
        message = ExtendedMessage.builder().setKey(key).build();
        timer = Timer.builder()
                .setMessageId("m1")
                .setEventTimestamp(0L)
                .setSystemTimestamp(0L)
                .setStreamId("s1")
                .setStreamSpecId(0L)
                .setTriggerTimestamp(0L)
                .setKey(key)
                .build();

        Map<String, StatesHolder<InternalState>> internal = new HashMap<>();
        Map<String, StatesHolder<ExternalState>> external = new HashMap<>();
        TableSchema stateSchema = TableSchema.builder()
                .addValue("count", TiType.int64())
                .build();
        external.put(EXT_STATE, new StatesHolder<>(EXT_STATE, keySchema, stateSchema));

        // Read-only joined state: a writer's value is pre-populated under joinedKey.
        joinedStateSchema = TableSchema.builder()
                .addValue("count", TiType.int64())
                .build();
        joinedKey = key;
        Map<String, StatesHolder<ExternalState>> joined = new HashMap<>();
        var joinedHolder = new StatesHolder<ExternalState>(JOINED_STATE, keySchema, joinedStateSchema);
        joinedHolder.set(
                joinedKey.getRow(),
                new ExternalState(new PayloadBuilder(joinedStateSchema).set("count", 42L).finish())
        );
        joined.put(JOINED_STATE, joinedHolder);

        var backend = new DefaultStateBackend(
                Set.of(PROTO_STATE, CUSTOM_STATE, RAW_STATE, YSON_STATE),
                Set.of(EXT_STATE),
                Set.of(JOINED_STATE),
                internal,
                external,
                joined,
                keySchema
        );
        ctx = new DefaultRuntimeContext(
                backend,
                /* streamSpecs — not exercised in state-descriptor tests */ null,
                Map.of(),
                0L,
                Map.of(),
                Map.of()
        );
    }

    @Test
    void rawDescriptorRoundTrip() {
        StateAccessor<byte[]> acc = ctx.getState(StateDescriptors.raw(RAW_STATE), message);
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        acc.set(payload);
        assertEquals("hello", new String(acc.get().orElseThrow(), StandardCharsets.UTF_8));
    }

    @Test
    void customDescriptorRoundTrip() {
        StateAccessor<String> acc = ctx.getState(
                StateDescriptors.customWithoutDefault(
                        CUSTOM_STATE,
                        String.class,
                        new ByteArrayCodec<String>() {
                            @Override
                            public byte[] encode(String s) {
                                return s.getBytes(StandardCharsets.UTF_8);
                            }

                            @Override
                            public String decode(byte[] bytes) {
                                return new String(bytes, StandardCharsets.UTF_8);
                            }
                        }
                ),
                message
        );
        acc.set("abc");
        assertEquals("abc", acc.get().orElseThrow());
    }

    @Test
    void externalDescriptorReadsConfiguredHolder() {
        ExternalStateAccessor acc = ctx.getState(StateDescriptors.external(EXT_STATE), message);
        assertNotNull(acc);
        // No value set → getOrDefault() returns an empty Payload with the state schema.
        Payload p = acc.getOrDefault();
        assertNotNull(p);
    }

    @Test
    void getOrDefaultPayloadIsSafeToModifyAcrossIterations() {
        ExternalStateAccessor acc = ctx.getState(StateDescriptors.external(EXT_STATE), message);

        // First iteration: take the default payload, edit "count" via a builder, finish.
        Payload firstDefault = acc.getOrDefault();
        Payload first = firstDefault.toBuilder().set("count", 1L).finish();

        // Second iteration: take the default again and edit the same field with a different value.
        Payload secondDefault = acc.getOrDefault();
        Payload second = secondDefault.toBuilder().set("count", 2L).finish();

        // The independently-modified payloads must differ: the memoized default returned by
        // getOrDefault() must not leak one caller's mutation into the next.
        assertNotEquals(first.get("count", Long.class), second.get("count", Long.class));
        assertEquals(1L, first.get("count", Long.class));
        assertEquals(2L, second.get("count", Long.class));
    }

    @Test
    void externalDescriptorRejectsNameWithoutLeadingSlash() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> StateDescriptors.external("state"));
    }

    @Test
    void externalDescriptorRejectsEmptyName() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> StateDescriptors.external(""));
    }

    @Test
    void externalDescriptorRejectsRootName() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> StateDescriptors.external("/"));
    }

    @Test
    void externalDescriptorRejectsTrailingSlash() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> StateDescriptors.external("/state/"));
    }

    @Test
    void externalDescriptorRejectsDoubleSlash() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> StateDescriptors.external("/foo//bar"));
    }

    /**
     * A user-defined {@link ByteArrayCodec} defined outside of {@code tech.ytsaurus.flow.state}.
     * It stores boolean-as-byte.
     */
    static final class FlagArrayCodec implements ByteArrayCodec<Boolean> {
        @Override
        public byte[] encode(Boolean b) {
            return new byte[]{(byte) (b ? 1 : 0)};
        }

        @Override
        public Boolean decode(byte[] bytes) {
            return bytes.length > 0 && bytes[0] == 1;
        }
    }

    @Test
    void customDescriptorOutsideFlowCorePackageWorks() {
        // Given: a user-defined byte codec from a different package (here the test package).
        // When: plugged into StateDescriptors.customWithoutDefault(...) and obtained via ctx.getState(...)
        StateAccessor<Boolean> acc = ctx.getState(
                StateDescriptors.customWithoutDefault(CUSTOM_STATE, Boolean.class, new FlagArrayCodec()), message);
        // Then: it is a regular state accessor with full functionality.
        assertInstanceOf(InternalStateAccessor.class, acc);
        acc.set(true);
        assertEquals(Boolean.TRUE, acc.get().orElseThrow());
        acc.set(false);
        assertEquals(Boolean.FALSE, acc.get().orElseThrow());
    }

    @Test
    void rawDescriptorWorksWithTimerKey() {
        StateAccessor<byte[]> acc = ctx.getState(StateDescriptors.raw(RAW_STATE), timer);
        acc.set(new byte[]{1, 2, 3});
        assertEquals(3, acc.get().orElseThrow().length);
    }

    static final class StringArrayCodec implements ByteArrayCodec<String> {
        @Override
        public byte[] encode(String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    @Test
    void getOrDefaultReturnsDescriptorDefaultWhenValueAbsent() {
        StateAccessor<String> acc = ctx.getState(
                StateDescriptors.custom(CUSTOM_STATE, String.class, new StringArrayCodec(), () -> "DEFAULT"),
                message);
        assertEquals("DEFAULT", acc.getOrDefault());
    }

    @Test
    void getOrDefaultReturnsStoredValueWhenPresent() {
        StateAccessor<String> acc = ctx.getState(
                StateDescriptors.custom(CUSTOM_STATE, String.class, new StringArrayCodec(), () -> "DEFAULT"),
                message);
        acc.set("stored");
        assertEquals("stored", acc.getOrDefault());
    }

    @Test
    void getOrDefaultReturnsEmptyArrayForRawStateWhenValueAbsent() {
        StateAccessor<byte[]> acc = ctx.getState(StateDescriptors.raw(RAW_STATE), message);
        assertArrayEquals(new byte[0], acc.getOrDefault());
    }

    @Test
    void getOrDefaultReturnsStoredValueForRawStateWhenValuePresent() {
        StateAccessor<byte[]> acc = ctx.getState(StateDescriptors.raw(RAW_STATE), message);
        acc.set(new byte[]{42});
        assertArrayEquals(new byte[]{42}, acc.getOrDefault());
    }

    @Test
    void customWithoutDefaultThrowsOnGetOrDefault() {
        StateAccessor<String> acc = ctx.getState(
                StateDescriptors.customWithoutDefault(CUSTOM_STATE, String.class, new StringArrayCodec()),
                message);
        assertThrows(UnsupportedOperationException.class, acc::getOrDefault);
    }

    @Entity
    static class CounterState {
        String name;
        Long count;
        List<String> tags;

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CounterState that = (CounterState) o;
            return Objects.equals(name, that.name)
                    && Objects.equals(count, that.count)
                    && Objects.equals(tags, that.tags);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, count, tags);
        }
    }

    @Test
    void ysonDescriptorRoundTripEntity() {
        StateAccessor<CounterState> acc = ctx.getState(
                StateDescriptors.yson(YSON_STATE, CounterState.class), message);

        var state = new CounterState();
        state.name = "flow";
        state.count = 7L;
        state.tags = List.of("a", "b");

        acc.set(state);
        assertEquals(state, acc.get().orElseThrow());
    }

    @Test
    void ysonDescriptorRoundTripPrimitive() {
        // A YTreeSerializerFactory-supported type (no @Entity) also round-trips through yson(...).
        StateAccessor<Long> acc = ctx.getState(
                StateDescriptors.yson(YSON_STATE, Long.class), message);

        acc.set(5L);
        assertEquals(5L, acc.get().orElseThrow());
    }

    @Test
    void ysonGetOrDefaultReturnsStoredValueWhenPresent() {
        StateAccessor<CounterState> acc = ctx.getState(
                StateDescriptors.yson(YSON_STATE, CounterState.class), message);

        var state = new CounterState();
        state.name = "stored";
        state.count = 1L;
        acc.set(state);

        assertEquals(state, acc.getOrDefault());
    }

    @Test
    void ysonGetOrDefaultThrowsWhenAbsent() {
        StateAccessor<CounterState> acc = ctx.getState(
                StateDescriptors.yson(YSON_STATE, CounterState.class), message);

        assertThrows(UnsupportedOperationException.class, acc::getOrDefault);
    }

    @Test
    void joinedExternalDescriptorReadsWriterValue() {
        ReadOnlyExternalStateAccessor acc =
                ctx.getState(StateDescriptors.externalReadOnly(JOINED_STATE), message);
        assertEquals(42L, acc.get().orElseThrow().get("count", Long.class));
        assertEquals(42L, acc.getOrDefault().get("count", Long.class));
    }

    @Test
    void joinedExternalDescriptorSetThrows() {
        ReadOnlyExternalStateAccessor acc =
                ctx.getState(StateDescriptors.externalReadOnly(JOINED_STATE), message);
        Payload value = new PayloadBuilder(joinedStateSchema).set("count", 7L).finish();
        assertThrows(UnsupportedOperationException.class, () -> acc.set(value));
    }

    @Test
    void joinedExternalDescriptorClearThrows() {
        ReadOnlyExternalStateAccessor acc =
                ctx.getState(StateDescriptors.externalReadOnly(JOINED_STATE), message);
        assertThrows(UnsupportedOperationException.class, acc::clear);
    }

    @Test
    void joinedExternalDescriptorMissingKeyGivesEmpty() {
        Payload otherKey = new PayloadBuilder(
                TableSchema.builder().addValue("k", TiType.string()).build()
        ).set("k", "absent").finish();
        ExtendedMessage otherMessage = ExtendedMessage.builder().setKey(otherKey).build();
        ReadOnlyExternalStateAccessor acc =
                ctx.getState(StateDescriptors.externalReadOnly(JOINED_STATE), otherMessage);
        Assertions.assertTrue(acc.get().isEmpty());
    }

    @Test
    void joinedExternalDescriptorRejectsUndeclaredName() {
        var descriptor = StateDescriptors.externalReadOnly("/not-declared");
        assertThrows(IllegalArgumentException.class, () -> ctx.getState(descriptor, message));
    }
}
