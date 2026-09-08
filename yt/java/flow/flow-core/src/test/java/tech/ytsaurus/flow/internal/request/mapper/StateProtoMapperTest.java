package tech.ytsaurus.flow.internal.request.mapper;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.state.DefaultStateBackend;
import tech.ytsaurus.flow.state.DefaultStateManager;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.state.StateFormat;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.test.TTestMessage;
import tech.ytsaurus.flow.utils.YsonUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateProtoMapperTest {

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .add(new ColumnSchema("id", ColumnValueType.UINT64))
            .build();
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .add(new ColumnSchema("count", ColumnValueType.INT64))
            .build();
    private static final String PROTO_STATE = "/proto";
    private static final String PROTO_TYPE = TTestMessage.getDefaultInstance()
            .getDescriptorForType().getFullName();
    private static final CodecRegistry CODECS = CodecRegistry.DEFAULT;

    private static Payload keyPayload(long id) {
        return new PayloadBuilder(KEY_SCHEMA).set("id", id).finish();
    }

    private static ByteString key(long id) {
        var row = new UnversionedRow(List.of(new UnversionedValue(0, ColumnValueType.UINT64, false, id)));
        return CODECS.getKeyCodec().encode(row);
    }

    private static TStateItem resetItem(long id) {
        return TStateItem.newBuilder().setKey(key(id)).setReset(true).build();
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

    private static StatesHolder protoHolder() {
        return new StatesHolder(
                PROTO_STATE, KEY_SCHEMA, null, StateFormat.PROTO, PROTO_TYPE);
    }

    /**
     * A proto state accessor over {@code holder}, built the way the runtime builds it.
     */
    private static StateAccessor<TTestMessage> protoAccessor(StatesHolder holder, Payload key) {
        var backend = new DefaultStateBackend(
                Set.of(), Set.of(PROTO_STATE), Set.of(),
                Map.of(), Map.of(PROTO_STATE, holder), Map.of(),
                KEY_SCHEMA);
        return new DefaultStateManager(backend)
                .create(StateDescriptors.externalProto(PROTO_STATE, TTestMessage.class), () -> key);
    }

    @Test
    void undecodableExternalStateValueDoesNotFailIntake() {
        // Values are carried as opaque wire bytes: intake never parses them, so garbage only
        // blows up when a computation actually reads it.
        var mapper = new ExternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        var garbage = TStateItem.newBuilder()
                .setKey(key(1))
                .setReset(false)
                .setState(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
        TState proto = TState.newBuilder()
                .setName("ext")
                .setSchema(YsonUtils.protoFromYTree(STATE_SCHEMA.toYTree()))
                .addStateItems(garbage)
                .build();

        var states = mapper.fromProto(List.of(proto), GUID.create(), GUID.create());

        var holder = states.get("ext");
        var loaded = List.copyOf(holder.getStates().values());
        assertEquals(1, loaded.size());
        assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), loaded.get(0).getBytes());
    }

    @Test
    void externalStateValueBytesAreSentBackUnchanged() {
        var mapper = new ExternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        var holder = new StatesHolder("ext", KEY_SCHEMA, STATE_SCHEMA);
        var bytes = CODECS.getPayloadCodec().codecFor(STATE_SCHEMA)
                .encode(new PayloadBuilder(STATE_SCHEMA).set("count", 7L).finish());
        holder.set(CODECS.getKeyCodec().decode(key(1)), new State(bytes));

        var proto = mapper.toProto(holder);

        assertEquals(STATE_SCHEMA, TableSchema.fromYTree(YsonUtils.yTreeFromProto(proto.getSchema())));
        assertEquals(1, proto.getStateItemsCount());
        assertEquals(bytes, proto.getStateItems(0).getState());
    }

    @Test
    void externalResetOnlyStateWithEmptySchemaLoadsReset() {
        // A reset-only external state may carry an empty schema (companion_service.proto): it must be
        // accepted and loaded as RESET rather than rejected for the missing schema.
        var mapper = new ExternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        TState proto = TState.newBuilder().setName("ext").addStateItems(resetItem(1)).build();

        var states = mapper.fromProto(List.of(proto), GUID.create(), GUID.create());

        assertEquals(List.of(State.RESET), List.copyOf(states.get("ext").getStates().values()));
    }

    @Test
    void externalNonResetStateWithEmptySchemaThrows() {
        // The schema is only optional for reset items; a non-reset item still requires it.
        var mapper = new ExternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        TStateItem nonReset = TStateItem.newBuilder().setKey(key(1)).setReset(false)
                .setState(ByteString.EMPTY).build();
        TState proto = TState.newBuilder().setName("ext").addStateItems(nonReset).build();

        assertThrows(IllegalArgumentException.class,
                () -> mapper.fromProto(List.of(proto), GUID.create(), GUID.create()));
    }

    @Test
    void internalResetItemLoadsCanonicalReset() {
        // A reset item carries no value; it must decode to State.RESET (null value), not a
        // non-null empty value, so the round-trip preserves equality with State.RESET.
        var mapper = new InternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        TState proto = TState.newBuilder().setName("int").addStateItems(resetItem(1)).build();

        var states = mapper.fromProto(List.of(proto), GUID.create(), GUID.create());
        assertEquals(List.of(State.RESET), List.copyOf(states.get("int").getStates().values()));
    }

    @Test
    void internalStateValueBytesAreLoadedUnchanged() {
        // Values are carried as opaque wire bytes: intake never decodes them, and an internal
        // state needs no schema for them.
        var mapper = new InternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        var bytes = ByteString.copyFrom(new byte[]{1, 2, 3});
        var item = TStateItem.newBuilder().setKey(key(1)).setReset(false).setState(bytes).build();
        TState proto = TState.newBuilder().setName("int").addStateItems(item).build();

        var states = mapper.fromProto(List.of(proto), GUID.create(), GUID.create());

        var state = states.get("int").getStates().get(CODECS.getKeyCodec().decode(key(1)));
        assertFalse(state.isReset());
        assertEquals(bytes, state.getBytes());
    }

    @Test
    void internalStateValueBytesAreSentBackUnchanged() {
        var mapper = new InternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        var holder = new StatesHolder("int", KEY_SCHEMA, null);
        var bytes = ByteString.copyFrom(new byte[]{1, 2, 3});
        holder.set(CODECS.getKeyCodec().decode(key(1)), new State(bytes));

        var proto = mapper.toProto(holder);

        assertTrue(proto.getSchema().isEmpty());
        assertEquals(1, proto.getStateItemsCount());
        assertEquals(bytes, proto.getStateItems(0).getState());
    }

    @Test
    void mapperRoundTripsProtoFormat() {
        var mapper = new ExternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        var key = keyPayload(1);

        var protoState = TState.newBuilder()
                .setName(PROTO_STATE)
                .setFormat(StateFormat.PROTO.getWireValue())
                .setProtoType(PROTO_TYPE)
                .addStateItems(TStateItem.newBuilder()
                        .setKey(CODECS.getKeyCodec().encode(key.getRow()))
                        .setReset(false)
                        .setState(ByteString.copyFrom(message(42).toByteArray())))
                .build();

        var holders = mapper.fromProto(List.of(protoState), GUID.create(), GUID.create());
        var holder = holders.get(PROTO_STATE);
        assertEquals(StateFormat.PROTO, holder.getFormat());
        assertEquals(PROTO_TYPE, holder.getProtoType());

        var acc = protoAccessor(holder, key);
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
        var mapper = new ExternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());

        var holder = protoHolder();
        // An all-default message serializes to zero bytes; it must still be sent.
        protoAccessor(holder, keyPayload(1)).set(TTestMessage.getDefaultInstance());
        TState out = mapper.toProto(holder);
        assertEquals(1, out.getStateItemsCount());
        assertFalse(out.getStateItems(0).getReset());
        assertTrue(out.getStateItems(0).getState().isEmpty());
    }

    private static TTestMessage parse(ByteString bytes) {
        try {
            return TTestMessage.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
