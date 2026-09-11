package tech.ytsaurus.flow.state;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.context.DefaultRuntimeContext;
import tech.ytsaurus.flow.internal.request.mapper.InternalStateProtoMapper;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.test.TOptionalTestMessage;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for internal state tracking: the value handed out by {@link InternalStateAccessor} is live
 * and written back when the holder's modified states are collected, while unchanged states and
 * states read through {@link StateAccessor#readOnly()} or {@link InternalStateDescriptor#readOnly()}
 * produce no write.
 */
class InternalStateTrackingTest {
    private static final String COUNTER_STATE = "counter-state";
    private static final String RAW_STATE = "raw-state";
    private static final String PROTO_STATE = "proto-state";
    private static final CounterCodec CODEC = new CounterCodec();
    private static final InternalStateDescriptor<Counter> COUNTER =
            StateDescriptors.custom(COUNTER_STATE, Counter.class, CODEC, Counter::new);
    private static final InternalStateDescriptor<Counter> COUNTER_READ_ONLY = COUNTER.readOnly();
    private static final InternalStateDescriptor<byte[]> RAW = StateDescriptors.raw(RAW_STATE);
    private static final InternalStateDescriptor<TOptionalTestMessage> PROTO =
            StateDescriptors.protobuf(PROTO_STATE, TOptionalTestMessage.class);

    private TableSchema keySchema;
    private ExtendedMessage message;
    private Map<String, StatesHolder> internalStates;
    private DefaultRuntimeContext ctx;

    /**
     * Mutable state value: user code changes {@code value} in place.
     */
    static final class Counter {
        long value;
    }

    static final class CounterCodec implements ByteArrayCodec<Counter> {
        @Override
        public byte[] encode(Counter value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(value.value).array();
        }

        @Override
        public Counter decode(byte[] bytes) {
            var counter = new Counter();
            counter.value = ByteBuffer.wrap(bytes).getLong();
            return counter;
        }
    }

    @BeforeEach
    void setUp() {
        keySchema = TableSchema.builder()
                .addValue("k", TiType.string())
                .build();
        message = message("k1");
        internalStates = new HashMap<>();
        var backend = new DefaultStateBackend(
                Set.of(COUNTER_STATE, RAW_STATE, PROTO_STATE),
                Set.of(),
                Set.of(),
                internalStates,
                new HashMap<>(),
                new HashMap<>(),
                keySchema
        );
        ctx = new DefaultRuntimeContext(
                backend,
                /*streamSpecs — not exercised in state tests*/ null,
                Map.of(),
                0L,
                Map.of(),
                Map.of()
        );
    }

    @Test
    @DisplayName("value mutated in place is written back")
    void mutationIsWrittenBack() {
        seed(message, 1L);

        ctx.getState(COUNTER, message).getOrDefault().value += 1;

        assertEquals(wire(2L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("reading without mutating writes nothing")
    void readingWritesNothing() {
        seed(message, 1L);

        assertEquals(1L, ctx.getState(COUNTER, message).get().value);

        assertTrue(modified().isEmpty());
    }

    @Test
    @DisplayName("a value identical to the incoming one writes nothing")
    void identicalValueWritesNothing() {
        seed(message, 1L);

        ctx.getState(COUNTER, message).getOrDefault().value = 1L;

        assertTrue(modified().isEmpty());
    }

    @Test
    @DisplayName("clear() of an existing state emits a reset")
    void clearOfExistingStateEmitsReset() {
        seed(message, 1L);

        var state = ctx.getState(COUNTER, message);
        state.clear();
        assertNull(state.get());

        assertTrue(modified().get(key(message)).isReset());
    }

    @Test
    @DisplayName("getOrDefault() after clear() revives the state")
    void getOrDefaultAfterClearRevivesTheState() {
        seed(message, 1L);

        var state = ctx.getState(COUNTER, message);
        state.clear();
        state.getOrDefault().value = 5L;

        assertEquals(wire(5L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("getOrDefault() after clear() left untouched keeps the reset")
    void getOrDefaultAfterClearLeftUntouchedKeepsTheReset() {
        seed(message, 1L);

        var state = ctx.getState(COUNTER, message);
        state.clear();
        state.getOrDefault();

        assertTrue(modified().get(key(message)).isReset());
    }

    @Test
    @DisplayName("getOrDefault() of an absent state left untouched writes nothing")
    void getOrDefaultOfAbsentStateWritesNothing() {
        assertEquals(0L, ctx.getState(COUNTER, message).getOrDefault().value);

        assertTrue(modified().isEmpty());
        assertEquals(0, proto(COUNTER_STATE).getStateItemsCount());
    }

    @Test
    @DisplayName("getOrDefault() materializes an absent state, so changes to it are written back")
    void getOrDefaultMaterializesAbsentState() {
        ctx.getState(COUNTER, message).getOrDefault().value = 7L;

        assertEquals(wire(7L), modified().get(key(message)).getBytes());

        TState out = proto(COUNTER_STATE);
        assertEquals(1, out.getStateItemsCount());
        assertFalse(out.getStateItems(0).getReset());
        assertEquals(wire(7L), out.getStateItems(0).getState());
    }

    @Test
    @DisplayName("getOrDefault() attaches the default, so a later get() hands out the same value")
    void getOrDefaultAttachesTheDefault() {
        var state = ctx.getState(COUNTER, message);
        var value = state.getOrDefault();

        assertSame(value, state.get());
        assertSame(value, ctx.getState(COUNTER, message).getOrDefault());
        assertTrue(modified().isEmpty());
    }

    @Test
    @DisplayName("a raw default changed in place is written back")
    void rawGetOrDefaultChangedInPlaceIsWrittenBack() {
        // A byte[] default is mutable, so attaching it is what lets a change reach the wire.
        ctx.getState(RAW, message).getOrDefault(new byte[]{1, 2, 3})[0] = 42;

        assertEquals(
                ByteString.copyFrom(new byte[]{42, 2, 3}),
                modifiedOf(RAW_STATE).get(key(message)).getBytes());
    }

    @Test
    @DisplayName("getOrDefault() of an absent raw state sends no empty payload")
    void rawGetOrDefaultSendsNothing() {
        // The raw default is an empty byte array and encodes to an empty payload, which the
        // worker rejects on a non-reset item; attaching it keeps the item off the wire.
        assertEquals(0, ctx.getState(RAW, message).getOrDefault().length);

        assertTrue(modifiedOf(RAW_STATE).isEmpty());
        assertEquals(0, proto(RAW_STATE).getStateItemsCount());
    }

    @Test
    @DisplayName("a value that encodes to no bytes goes out as a reset")
    void emptyValueGoesOutAsReset() {
        // An all-default protobuf message serializes to zero bytes, and the worker rejects a
        // non-reset item with an empty payload: no bytes is no value, and a reset is how the
        // wire spells that. Only an external state in the proto format is exempt, see
        // StateProtoMapperTest.
        ctx.getState(PROTO, message).set(TOptionalTestMessage.getDefaultInstance());

        TState out = proto(PROTO_STATE);
        assertEquals(1, out.getStateItemsCount());
        assertTrue(out.getStateItems(0).getReset());
        assertTrue(out.getStateItems(0).getState().isEmpty());
    }

    @Test
    @DisplayName("getOrDefault(value) binds the given value, so changes to it are written back")
    void getOrDefaultBindsTheGivenValue() {
        var initial = new Counter();
        ctx.getState(COUNTER, message).getOrDefault(initial).value = 4L;

        assertEquals(wire(4L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("set() replaces the live value")
    void setReplacesTheLiveValue() {
        seed(message, 1L);

        var state = ctx.getState(COUNTER, message);
        var replacement = new Counter();
        replacement.value = 9L;
        state.set(replacement);
        assertSame(replacement, state.get());

        assertEquals(wire(9L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("a change made after set() is written back")
    void mutationAfterSetIsWrittenBack() {
        var state = ctx.getState(COUNTER, message);
        var value = new Counter();
        state.set(value);
        value.value = 6L;

        assertEquals(wire(6L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("readOnly(): reading writes nothing")
    void readOnlyReadWritesNothing() {
        seed(message, 1L);

        var value = ctx.getState(COUNTER, message).readOnly().get();
        assertEquals(1L, value.value);
        value.value = 2L;

        assertTrue(modified().isEmpty());
    }

    @Test
    @DisplayName("readOnly(): getOrDefault() does not create the state")
    void readOnlyGetOrDefaultDoesNotCreateTheState() {
        var state = ctx.getState(COUNTER, message).readOnly();
        assertEquals(0L, state.getOrDefault().value);
        assertEquals(3L, state.getOrDefault(counter(3L)).value);

        assertNull(state.get());
        assertTrue(modified().isEmpty());
    }

    @Test
    @DisplayName("readOnly(): set() and clear() throw")
    void readOnlyRejectsWrites() {
        var state = ctx.getState(COUNTER, message).readOnly();

        assertThrows(UnsupportedOperationException.class, () -> state.set(counter(1L)));
        assertThrows(UnsupportedOperationException.class, state::clear);
        assertSame(state, state.readOnly());
    }

    @Test
    @DisplayName("readOnly() returns the value the writable accessor handed out")
    void readOnlyReturnsTheLiveValue() {
        seed(message, 1L);

        var state = ctx.getState(COUNTER, message);
        var readOnly = state.readOnly().get();
        assertSame(readOnly, state.get());
        readOnly.value = 2L;

        assertEquals(wire(2L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("read-only descriptor: reading writes nothing")
    void readOnlyDescriptorReadWritesNothing() {
        seed(message, 1L);

        var value = ctx.getState(COUNTER_READ_ONLY, message).get();
        assertEquals(1L, value.value);
        value.value = 2L;

        assertTrue(modified().isEmpty());
    }

    @Test
    @DisplayName("read-only descriptor: getOrDefault() does not create the state")
    void readOnlyDescriptorGetOrDefaultDoesNotCreateTheState() {
        var state = ctx.getState(COUNTER_READ_ONLY, message);
        assertEquals(0L, state.getOrDefault().value);
        assertEquals(3L, state.getOrDefault(counter(3L)).value);

        assertNull(state.get());
        assertTrue(modified().isEmpty());
    }

    @Test
    @DisplayName("read-only descriptor: set() and clear() throw")
    void readOnlyDescriptorRejectsWrites() {
        var state = ctx.getState(COUNTER_READ_ONLY, message);

        assertThrows(UnsupportedOperationException.class, () -> state.set(counter(1L)));
        assertThrows(UnsupportedOperationException.class, state::clear);
        assertSame(state, state.readOnly());
    }

    @Test
    @DisplayName("read-only descriptor reads the cell the writable descriptor writes")
    void readOnlyDescriptorSharesTheStateCell() {
        seed(message, 1L);

        ctx.getState(COUNTER, message).getOrDefault().value = 2L;

        assertEquals(2L, ctx.getState(COUNTER_READ_ONLY, message).get().value);
        assertEquals(wire(2L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("read-only descriptor does not untrack a state the writable accessor has read")
    void readOnlyDescriptorDoesNotUntrackTheState() {
        seed(message, 1L);

        ctx.getState(COUNTER, message).get();
        ctx.getState(COUNTER_READ_ONLY, message).get().value = 2L;

        assertEquals(wire(2L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("readOnly() carries the state identity and stays read-only")
    void readOnlyCarriesTheStateIdentity() {
        assertEquals(COUNTER.getName(), COUNTER_READ_ONLY.getName());
        assertEquals(COUNTER.getStateClass(), COUNTER_READ_ONLY.getStateClass());

        assertSame(COUNTER_READ_ONLY, COUNTER_READ_ONLY.readOnly());

        var state = ctx.getState(COUNTER_READ_ONLY, message);
        assertThrows(UnsupportedOperationException.class, () -> state.set(counter(1L)));
    }

    @Test
    @DisplayName("the same state and key give the same value")
    void sameKeyGivesSameValue() {
        seed(message, 1L);

        var first = ctx.getState(COUNTER, message);
        first.getOrDefault().value += 1;
        var second = ctx.getState(COUNTER, message);
        assertSame(first.get(), second.get());
        second.getOrDefault().value += 1;

        assertEquals(1, modified().size());
        assertEquals(wire(3L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("different keys of one state are tracked independently")
    void differentKeysAreTrackedIndependently() {
        var other = message("k2");
        seed(message, 1L);
        seed(other, 5L);

        ctx.getState(COUNTER, message).getOrDefault().value += 1;

        assertEquals(1, modified().size());
        assertEquals(wire(2L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("collectModifiedStates() picks up the changes made in place")
    void collectModifiedStatesPicksUpTheChanges() {
        seed(message, 1L);

        var value = ctx.getState(COUNTER, message).getOrDefault();
        value.value = 2L;

        var holder = holder(COUNTER_STATE);
        assertEquals(wire(2L), holder.collectModifiedStates().get(key(message)).getBytes());

        // A later change is picked up by the next call.
        value.value = 3L;
        assertEquals(wire(3L), holder.collectModifiedStates().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("reading a state whose stored bytes are not canonical rewrites it canonically")
    void readingNonCanonicalStoredBytesRewritesCanonically() {
        // Nine bytes where this codec would produce eight: what another writer, or another
        // encoding of the same value, may have left in the table.
        holder(COUNTER_STATE).load(
                key(message),
                new State(ByteString.copyFrom(new byte[]{0, 0, 0, 0, 0, 0, 0, 1, 99})));

        assertEquals(1L, ctx.getState(COUNTER, message).get().value);

        // A one-time self-healing write: the canonical bytes land in the table, after which the
        // stored and encoded forms match and later reads write nothing.
        assertEquals(wire(1L), modified().get(key(message)).getBytes());
    }

    @Test
    @DisplayName("a raw state mutated in place is written back")
    void rawStateMutationIsWrittenBack() {
        holder(RAW_STATE).load(key(message), new State(ByteString.copyFrom(new byte[]{1, 2, 3})));

        ctx.getState(RAW, message).get()[0] = 42;

        assertEquals(
                ByteString.copyFrom(new byte[]{42, 2, 3}),
                modifiedOf(RAW_STATE).get(key(message)).getBytes());
    }

    private ExtendedMessage message(String key) {
        return ExtendedMessage.builder()
                .setKey(new PayloadBuilder(keySchema).set("k", key).finish())
                .build();
    }

    private void seed(ExtendedMessage forMessage, long value) {
        holder(COUNTER_STATE).load(key(forMessage), new State(wire(value)));
    }

    private StatesHolder holder(String stateName) {
        return internalStates.computeIfAbsent(stateName, name -> new StatesHolder(name, keySchema));
    }

    private Map<UnversionedRow, State> modified() {
        return modifiedOf(COUNTER_STATE);
    }

    private Map<UnversionedRow, State> modifiedOf(String stateName) {
        return holder(stateName).collectModifiedStates();
    }

    private TState proto(String stateName) {
        var holder = holder(stateName);
        return new InternalStateProtoMapper(keySchema, CodecRegistry.getInstance().getKeyCodec())
                .toProto(holder, holder.collectModifiedStates());
    }

    private static UnversionedRow key(ExtendedMessage forMessage) {
        return forMessage.getKey().getRow();
    }

    private static ByteString wire(long value) {
        return CodecRegistry.getInstance().getInternalStateValueCodec().encode(CODEC.encode(counter(value)));
    }

    private static Counter counter(long value) {
        var counter = new Counter();
        counter.value = value;
        return counter;
    }
}
