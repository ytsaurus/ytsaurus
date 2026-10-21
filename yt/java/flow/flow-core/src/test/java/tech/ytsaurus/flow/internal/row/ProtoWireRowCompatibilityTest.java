package tech.ytsaurus.flow.internal.row;

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtoWireRowCompatibilityTest {
    @Test
    void mixedRowsAreCompatibleWithLegacyReadersAndWriters() {
        var types = new ColumnValueType[] {
                ColumnValueType.INT64, ColumnValueType.UINT64, ColumnValueType.DOUBLE,
                ColumnValueType.BOOLEAN, ColumnValueType.STRING, ColumnValueType.ANY,
                ColumnValueType.COMPOSITE, ColumnValueType.NULL
        };
        var values = new Object[] {-42L, 150L, -0.0, true,
                new byte[] {'x', 0}, new byte[] {'1'}, new byte[] {'[', '1', ';', ']'}, null};
        var schemaBuilder = TableSchema.builder();
        var legacyValues = new ArrayList<UnversionedValue>();
        var writer = new ProtoWireRowWriter(types.length);
        for (int i = 0; i < types.length; i++) {
            schemaBuilder.add(types[i] == ColumnValueType.COMPOSITE
                    ? new ColumnSchema("c" + i, TiType.list(TiType.int64()))
                    : new ColumnSchema("c" + i, types[i] == ColumnValueType.NULL
                            ? ColumnValueType.STRING : types[i]));
            legacyValues.add(new UnversionedValue(i, types[i], false, values[i]));
        }
        writer.writeInt64(0, -42);
        writer.writeUint64Bits(1, 150);
        writer.writeDouble(2, -0.0);
        writer.writeBoolean(3, true);
        writer.writeBytes(4, ColumnValueType.STRING, ByteString.copyFrom((byte[]) values[4]));
        writer.writeBytes(5, ColumnValueType.ANY, ByteString.copyFrom((byte[]) values[5]));
        writer.writeBytes(6, ColumnValueType.COMPOSITE, ByteString.copyFrom((byte[]) values[6]));
        writer.writeNull(7);
        var legacy = new UnversionedRow(legacyValues);
        var bytes = writer.finish();
        var schema = schemaBuilder.build();
        var legacyBytes = new ProtoWireProtocolWriter(legacy).getBinary();

        assertEquals(legacyBytes, bytes);
        assertEquals(legacy, new ProtoWireProtocolReader(bytes).readUnversionedRow());
        var row = ProtoWireRow.fromWire(legacyBytes, schema);
        var tree = YTree.builder();
        row.writeTo(tree);
        assertEquals(legacy.toYTreeMap(schema), tree.build());
        var copy = new ProtoWireRowWriter(types.length);
        for (int i = 0; i < types.length; i++) {
            copy.writeFrom(i, row, i);
        }
        assertEquals(legacyBytes, copy.finish());
    }

    /**
     * The compact writer encodes the cell type as a varint, like the C++ WriteRowValue; the legacy
     * writer uses one raw byte. They differ only for MAX (0xEF), the only type above 0x7F.
     */
    @Test
    void sentinelTypesMatchLegacyBytesExceptMax() {
        for (var type : new ColumnValueType[] {ColumnValueType.MIN, ColumnValueType.THE_BOTTOM, ColumnValueType.MAX}) {
            var legacy = new UnversionedRow(List.of(new UnversionedValue(0, type, false, null)));
            var writer = new ProtoWireRowWriter(1);
            writer.writeEntity(0, type);
            var compact = writer.finish();
            var legacyBytes = new ProtoWireProtocolWriter(legacy).getBinary();

            if (type == ColumnValueType.MAX) {
                assertEquals(ByteString.copyFrom(new byte[] {0, 1, 0, (byte) 0xef, 0x01}), compact);
                assertEquals(ByteString.copyFrom(new byte[] {0, 1, 0, (byte) 0xef}), legacyBytes);
            } else {
                assertEquals(legacyBytes, compact);
            }
        }
    }

    @Test
    void bothWritersMatchIndependentGoldenBytes() {
        var expected = ByteString.copyFrom(new byte[] {0, 2, 0, 3, 1, 1, 0x10, 2, 'o', 'k'});
        var legacy = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.INT64, false, -1L),
                new UnversionedValue(1, ColumnValueType.STRING, false, new byte[] {'o', 'k'})));
        var writer = new ProtoWireRowWriter(2);
        writer.writeInt64(0, -1);
        writer.writeBytes(1, ColumnValueType.STRING, ByteString.copyFromUtf8("ok"));

        assertEquals(expected, new ProtoWireProtocolWriter(legacy).getBinary());
        assertEquals(expected, writer.finish());
        assertEquals(legacy, new ProtoWireProtocolReader(expected).readUnversionedRow());
        var schema = TableSchema.builder().addValue("number", TiType.int64())
                .addValue("text", TiType.string()).build();
        var row = ProtoWireRow.fromWire(expected, schema);
        assertEquals(-1L, row.getInt64(0));
        assertEquals("ok", row.bytes(1).toStringUtf8());
    }
}
