package tech.ytsaurus.flow.internal.request.mapper;

import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.utils.YsonUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StateProtoMapperTest {

    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .add(new ColumnSchema("id", ColumnValueType.UINT64))
            .build();
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .add(new ColumnSchema("count", ColumnValueType.INT64))
            .build();
    private static final CodecRegistry CODECS = CodecRegistry.DEFAULT;

    private static ByteString key(long id) {
        var row = new UnversionedRow(List.of(new UnversionedValue(0, ColumnValueType.UINT64, false, id)));
        return CODECS.getKeyCodec().encode(row);
    }

    private static TStateItem resetItem(long id) {
        return TStateItem.newBuilder().setKey(key(id)).setReset(true).build();
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
        assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), loaded.get(0).getValue());
    }

    @Test
    void externalStateValueBytesAreSentBackUnchanged() {
        var mapper = new ExternalStateProtoMapper(KEY_SCHEMA, CODECS.getKeyCodec());
        var holder = new StatesHolder<ExternalState>("ext", KEY_SCHEMA, STATE_SCHEMA);
        var bytes = CODECS.getPayloadCodec().codecFor(STATE_SCHEMA)
                .encode(new PayloadBuilder(STATE_SCHEMA).set("count", 7L).finish());
        holder.set(CODECS.getKeyCodec().decode(key(1)), new ExternalState(bytes));

        var proto = mapper.toProto(holder);

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

        assertEquals(List.of(ExternalState.RESET), List.copyOf(states.get("ext").getStates().values()));
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
        // A reset item carries no value; it must decode to InternalState.RESET (null value), not a
        // non-null empty value, so the round-trip preserves equality with InternalState.RESET.
        var mapper = new InternalStateProtoMapper(
                KEY_SCHEMA, CODECS.getKeyCodec(), CODECS.getInternalStateValueCodec());
        TState proto = TState.newBuilder().setName("int").addStateItems(resetItem(1)).build();

        var states = mapper.fromProto(List.of(proto));

        assertEquals(List.of(InternalState.RESET), List.copyOf(states.get("int").getStates().values()));
    }
}
