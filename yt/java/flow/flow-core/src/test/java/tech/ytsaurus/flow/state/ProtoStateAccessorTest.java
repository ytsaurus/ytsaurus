package tech.ytsaurus.flow.state;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;
import tech.ytsaurus.flow.test.TTestMessage;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ProtoStateAccessor}.
 */
class ProtoStateAccessorTest {

    private static final String STATE_NAME = "/state";
    private static final String PROTO_TYPE = TTestMessage.getDefaultInstance()
            .getDescriptorForType().getFullName();
    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("k", TiType.string())
            .build();
    private static final ByteStringCodec<TTestMessage> CODEC =
            ProtoStateAccessor.codecOf(STATE_NAME, TTestMessage.getDefaultInstance());

    private static Payload key() {
        return new PayloadBuilder(KEY_SCHEMA).set("k", "k1").finish();
    }

    private static TTestMessage message(long count) {
        return TTestMessage.newBuilder()
                .setId("id")
                .setTime(1)
                .setCount(count)
                .setValue(0.5)
                .setIsValue(true)
                .build();
    }

    private static State state(TTestMessage message) {
        return new State(message.toByteString());
    }

    private static StatesHolder protoHolder() {
        return new StatesHolder(
                STATE_NAME, KEY_SCHEMA, null, StateFormat.PROTO, PROTO_TYPE);
    }

    private static ProtoStateAccessor<TTestMessage> accessor(StatesHolder holder) {
        return new ProtoStateAccessor<>(
                key(), holder, TTestMessage.class, TTestMessage.getDefaultInstance(), CODEC);
    }

    @Test
    void getParsesReceivedBytes() {
        var holder = protoHolder();
        holder.load(key().getRow(), state(message(7)));
        assertEquals(7, accessor(holder).get().getCount());
        // Reads do not mark the state modified.
        assertTrue(holder.collectModifiedStates().isEmpty());
    }

    @Test
    void repeatedGetsReuseTheParsedMessage() {
        var holder = protoHolder();
        holder.load(key().getRow(), state(message(7)));
        var acc = accessor(holder);
        // The parse of a received entry is cached: identical bytes are not
        // reparsed on every read of a hot key, even through another accessor
        // over the same descriptor codec.
        var first = acc.get();
        assertNotNull(first);
        assertSame(first, acc.get());
        assertSame(first, accessor(holder).get());
    }

    @Test
    void getReturnsEmptyWhenAbsent() {
        var acc = accessor(protoHolder());
        assertNull(acc.get());
        assertEquals(TTestMessage.getDefaultInstance(), acc.getOrDefault());
    }

    @Test
    void setMarksModifiedAndRoundTrips() {
        var holder = protoHolder();
        var acc = accessor(holder);
        acc.set(message(42));
        assertFalse(holder.collectModifiedStates().isEmpty());
        assertEquals(message(42), acc.get());
        assertEquals(message(42).toByteString(), holder.get(key().getRow()).getBytes());
    }

    @Test
    void clearResets() {
        var holder = protoHolder();
        holder.load(key().getRow(), state(message(7)));
        var acc = accessor(holder);
        acc.clear();
        assertNull(acc.get());
        var modifiedStates = holder.collectModifiedStates();
        assertEquals(1, modifiedStates.size());
        assertTrue(modifiedStates.values().stream().allMatch(State::isReset));
    }

    @Test
    void protoTypeMismatchIsRejected() {
        var holder = new StatesHolder(
                STATE_NAME, KEY_SCHEMA, null, StateFormat.PROTO, "Some.Other.Type");
        var exception = assertThrows(IllegalStateException.class, () -> accessor(holder));
        assertTrue(exception.getMessage().contains("Some.Other.Type"));
        assertTrue(exception.getMessage().contains(PROTO_TYPE));
    }

    @Test
    void rowFormatHolderWithDataIsRejected() {
        // A SIMPLE_ROW holder carrying a state schema definitely holds wire
        // rows; a proto accessor over it is a descriptor/holder mismatch.
        var holder = new StatesHolder(
                STATE_NAME,
                KEY_SCHEMA,
                TableSchema.builder().addValue("count", TiType.int64()).build());
        var exception = assertThrows(IllegalStateException.class, () -> accessor(holder));
        assertTrue(exception.getMessage().contains("row wire format"));
    }

    @Test
    void schemalessRowFormatFallbackHolderIsAccepted() {
        // Absent-state fallback holders are built format-agnostic
        // (SIMPLE_ROW, no schema); the proto accessor must accept them and
        // read the state as absent.
        var holder = new StatesHolder(STATE_NAME, KEY_SCHEMA, null);
        assertNull(accessor(holder).get());
    }

    @Test
    void malformedBytesAreRejected() {
        var holder = protoHolder();
        holder.load(key().getRow(), new State(ByteString.copyFrom(new byte[] {(byte) 0xFF, 0x01, 0x02})));
        var exception = assertThrows(IllegalStateException.class, () -> accessor(holder).get());
        assertTrue(exception.getMessage().contains(STATE_NAME));
    }

    @Test
    void readOnlyAccessorRejectsWrites() {
        var holder = protoHolder();
        holder.load(key().getRow(), state(message(7)));
        var acc = new ReadOnlyProtoStateAccessor<>(
                key(), holder, TTestMessage.class, TTestMessage.getDefaultInstance(), CODEC);
        assertEquals(7, acc.get().getCount());
        assertThrows(UnsupportedOperationException.class, () -> acc.set(message(1)));
        assertThrows(UnsupportedOperationException.class, acc::clear);
    }

    @Test
    void descriptorFactoriesConstruct() {
        // Construction resolves the default instance reflectively and validates the name.
        assertEquals(STATE_NAME, StateDescriptors.externalProto(STATE_NAME, TTestMessage.class).getName());
        assertEquals(STATE_NAME,
                StateDescriptors.externalProtoReadOnly(STATE_NAME, TTestMessage.class).getName());
    }

    @Test
    void setKeepsTheMessageAndSerializesOnFirstByteRead() {
        var holder = new StatesHolder(STATE_NAME, KEY_SCHEMA, null, StateFormat.PROTO, PROTO_TYPE);
        var acc = accessor(holder);
        var message = TTestMessage.newBuilder().setId("a").setTime(1).setCount(2).setValue(3).setIsValue(true).build();
        acc.set(message);
        assertSame(message, acc.get());
        assertEquals(message.toByteString(), holder.get(key().getRow()).getBytes());
    }
}
