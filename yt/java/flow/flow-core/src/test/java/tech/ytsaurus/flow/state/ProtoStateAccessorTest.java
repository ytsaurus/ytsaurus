package tech.ytsaurus.flow.state;

import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.internal.request.mapper.ExternalStateProtoMapper;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.test.TTestMessage;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ProtoStateAccessor} and the proto-format branches of
 * {@link ExternalStateProtoMapper}.
 */
class ProtoStateAccessorTest {

    private static final String STATE_NAME = "/state";
    private static final String PROTO_TYPE = TTestMessage.getDefaultInstance()
            .getDescriptorForType().getFullName();
    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("k", TiType.string())
            .build();

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

    private static StatesHolder<ExternalState> protoHolder() {
        return new StatesHolder<>(STATE_NAME, KEY_SCHEMA, null, StateFormat.PROTO, PROTO_TYPE);
    }

    private static ProtoStateAccessor<TTestMessage> accessor(StatesHolder<ExternalState> holder) {
        return new ProtoStateAccessor<>(
                key(), holder, TTestMessage.class, TTestMessage.getDefaultInstance());
    }

    @Test
    void getParsesReceivedBytes() {
        var holder = protoHolder();
        holder.load(key().getRow(), new ProtoExternalState(message(7).toByteArray()));
        assertEquals(7, accessor(holder).get().orElseThrow().getCount());
        // Reads do not mark the state modified.
        assertFalse(holder.hasModifiedStates());
    }

    @Test
    void repeatedGetsReuseTheParsedMessage() {
        var holder = protoHolder();
        holder.load(key().getRow(), new ProtoExternalState(message(7).toByteArray()));
        var acc = accessor(holder);
        // The parse of a received entry is cached: identical bytes are not
        // reparsed on every read of a hot key.
        var first = acc.get().orElseThrow();
        assertSame(first, acc.get().orElseThrow());
    }

    @Test
    void mutatingTheSourceArrayCannotDesyncTheEntry() {
        var holder = protoHolder();
        byte[] bytes = message(7).toByteArray();
        var state = new ProtoExternalState(bytes);
        holder.load(key().getRow(), state);
        var acc = accessor(holder);
        assertEquals(7, acc.get().orElseThrow().getCount());

        // The entry copied the array: mutating it changes neither later
        // reads nor the serialized payload.
        java.util.Arrays.fill(bytes, (byte) 0);
        assertEquals(7, acc.get().orElseThrow().getCount());
        assertEquals(message(7).toByteString(), state.serialize());
    }

    @Test
    void getReturnsEmptyWhenAbsent() {
        var acc = accessor(protoHolder());
        assertTrue(acc.get().isEmpty());
        assertEquals(TTestMessage.getDefaultInstance(), acc.getOrDefault());
    }

    @Test
    void setMarksModifiedAndRoundTrips() {
        var holder = protoHolder();
        var acc = accessor(holder);
        acc.set(message(42));
        assertTrue(holder.hasModifiedStates());
        assertEquals(42, acc.get().orElseThrow().getCount());
    }

    @Test
    void clearResets() {
        var holder = protoHolder();
        holder.load(key().getRow(), new ProtoExternalState(message(7).toByteArray()));
        var acc = accessor(holder);
        acc.clear();
        assertTrue(acc.get().isEmpty());
        assertTrue(holder.getModifiedStates().values().stream().allMatch(State::isReset));
    }

    @Test
    void protoTypeMismatchIsRejected() {
        var holder = new StatesHolder<ExternalState>(
                STATE_NAME, KEY_SCHEMA, null, StateFormat.PROTO, "Some.Other.Type");
        var exception = assertThrows(IllegalStateException.class, () -> accessor(holder));
        assertTrue(exception.getMessage().contains("Some.Other.Type"));
        assertTrue(exception.getMessage().contains(PROTO_TYPE));
    }

    @Test
    void rowFormatHolderWithDataIsRejected() {
        // A SIMPLE_ROW holder carrying a state schema definitely holds wire
        // rows; a proto accessor over it is a descriptor/holder mismatch.
        var holder = new StatesHolder<ExternalState>(
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
        var holder = new StatesHolder<ExternalState>(STATE_NAME, KEY_SCHEMA, null);
        assertTrue(accessor(holder).get().isEmpty());
    }

    @Test
    void malformedBytesAreRejected() {
        var holder = protoHolder();
        holder.load(key().getRow(), new ProtoExternalState(new byte[] {(byte) 0xFF, 0x01, 0x02}));
        assertThrows(IllegalStateException.class, () -> accessor(holder).get());
    }

    @Test
    void readOnlyAccessorRejectsWrites() {
        var holder = protoHolder();
        holder.load(key().getRow(), new ProtoExternalState(message(7).toByteArray()));
        var acc = new ReadOnlyProtoStateAccessor<>(
                key(), holder, TTestMessage.class, TTestMessage.getDefaultInstance());
        assertEquals(7, acc.get().orElseThrow().getCount());
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
    void mapperRoundTripsProtoFormat() {
        var codecRegistry = CodecRegistry.getInstance();
        var mapper = new ExternalStateProtoMapper(
                KEY_SCHEMA, codecRegistry.getKeyCodec(), codecRegistry.getPayloadCodec());

        var protoState = TState.newBuilder()
                .setName(STATE_NAME)
                .setFormat(StateFormat.PROTO.getWireValue())
                .setProtoType(PROTO_TYPE)
                .addStateItems(TStateItem.newBuilder()
                        .setKey(codecRegistry.getKeyCodec().encode(key().getRow()))
                        .setReset(false)
                        .setState(ByteString.copyFrom(message(42).toByteArray())))
                .build();

        var holders = mapper.fromProto(List.of(protoState), GUID.create(), GUID.create());
        var holder = holders.get(STATE_NAME);
        assertEquals(StateFormat.PROTO, holder.getFormat());
        assertEquals(PROTO_TYPE, holder.getProtoType());

        var acc = accessor(holder);
        assertEquals(42, acc.get().orElseThrow().getCount());

        // Write back and map to the response: format and proto type are stamped.
        acc.set(message(43));
        TState out = mapper.toProto(holder);
        assertEquals(StateFormat.PROTO.getWireValue(), out.getFormat());
        assertEquals(PROTO_TYPE, out.getProtoType());
        assertEquals(1, out.getStateItemsCount());
        assertEquals(43, parse(out.getStateItems(0).getState()).getCount());
    }

    @Test
    void mapperSendsEmptyMessagePayload() {
        var codecRegistry = CodecRegistry.getInstance();
        var mapper = new ExternalStateProtoMapper(
                KEY_SCHEMA, codecRegistry.getKeyCodec(), codecRegistry.getPayloadCodec());

        var holder = protoHolder();
        // An all-default message serializes to zero bytes; it must still be sent.
        holder.set(key().getRow(), new ProtoExternalState(TTestMessage.newBuilder().buildPartial()));
        TState out = mapper.toProto(holder);
        assertEquals(1, out.getStateItemsCount());
        assertFalse(out.getStateItems(0).getReset());
        assertTrue(out.getStateItems(0).getState().isEmpty());
    }

    private static TTestMessage parse(ByteString bytes) {
        try {
            return TTestMessage.parseFrom(bytes);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
