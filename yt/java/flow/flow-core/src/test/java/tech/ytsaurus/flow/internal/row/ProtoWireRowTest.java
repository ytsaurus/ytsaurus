package tech.ytsaurus.flow.internal.row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtoWireRowTest {
    private static final String TRUNCATED = "truncated value";
    private static final String INVALID_VARINT = "invalid varint";

    /**
     * Builds {@code version=0 | cellCount=1 | columnId=0 | type | value} by hand, so tests can
     * supply malformed or non-canonical value bytes.
     */
    private static byte[] singleCellRow(ColumnValueType type, byte... value) {
        byte[] data = new byte[4 + value.length];
        data[1] = 1;
        data[3] = (byte) type.getValue();
        System.arraycopy(value, 0, data, 4, value.length);
        return data;
    }

    /**
     * Returns a varint of {@code length} bytes that encodes zero: continuation bytes then 0.
     */
    private static byte[] overlongZero(int length) {
        byte[] value = new byte[length];
        Arrays.fill(value, 0, length - 1, (byte) 0x80);
        return value;
    }

    private static void assertMalformed(String message, ByteString bytes, TableSchema schema) {
        var error = assertThrows(MalformedRowException.class, () -> ProtoWireRowDecoder.decode(bytes, schema));
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static void assertInvalid(String message, byte[] bytes, TableSchema schema) {
        var error = assertThrows(InvalidRowException.class,
                () -> ProtoWireRowDecoder.decode(ByteString.copyFrom(bytes), schema));
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    /**
     * Row content errors have their own unchecked types, which extend IllegalArgumentException
     * and IllegalStateException.
     */
    @Test
    void reportsRowContentErrorsWithOwnExceptionTypes() {
        var schema = TableSchema.builder().addValue("value", TiType.uint64()).build();
        byte uint64 = (byte) ColumnValueType.UINT64.getValue();

        assertMalformed(TRUNCATED, ByteString.copyFrom(new byte[] {0, 1, 0, uint64}), schema);
        assertMalformed(INVALID_VARINT,
                ByteString.copyFrom(new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10}), schema);
        assertMalformed("Unsupported value type 0x7f", ByteString.copyFrom(new byte[] {0, 1, 0, 0x7f}), schema);

        assertInvalid("does not support versions", new byte[] {1, 1, 0, uint64, 1}, schema);
        assertInvalid("Row cell count does not match schema", new byte[] {0, 2, 0, uint64, 1, 1, uint64, 1}, schema);
        assertInvalid("Column id does not match its position", new byte[] {0, 1, 1, uint64, 1}, schema);
        assertInvalid("Column type does not match schema",
                new byte[] {0, 1, 0, (byte) ColumnValueType.INT64.getValue(), 1}, schema);
        assertInvalid("Trailing bytes", new byte[] {0, 1, 0, uint64, 1, 0}, schema);

        assertThrows(MalformedRowException.class,
                () -> ProtoWireRowDecoder.decodeSchemaUnbound(ByteString.copyFrom(new byte[] {0, 1, 0})));
        assertThrows(InvalidRowException.class,
                () -> ProtoWireRowDecoder.decodeSchemaUnbound(ByteString.copyFrom(new byte[] {0, 0, 0})));

        assertTrue(IllegalArgumentException.class.isAssignableFrom(MalformedRowException.class));
        assertTrue(IllegalStateException.class.isAssignableFrom(InvalidRowException.class));
    }

    @Test
    void denseColumnsIncludeExplicitNulls() {
        var schema = TableSchema.builder()
                .addValue("c0", TiType.uint64())
                .addValue("c1", TiType.uint64())
                .addValue("c2", TiType.uint64())
                .build();
        var writer = new ProtoWireRowWriter(3);
        writer.writeUint64Bits(0, 42);
        writer.writeNull(1);
        writer.writeUint64Bits(2, 64);
        var row = ProtoWireRowDecoder.decode(writer.finish(), schema);

        assertEquals(42, row.getUint64Bits(0));
        assertTrue(row.isNull(1));
        assertFalse(row.isNull(2));
        assertEquals(64, row.getUint64Bits(2));
        assertThrows(IllegalArgumentException.class, () -> row.getUint64Bits(3));
        assertThrows(IllegalArgumentException.class, () -> row.type(-1));
        var tree = YTree.builder();
        row.writeTo(tree);
        assertEquals(Set.of("c0", "c1", "c2"), tree.build().asMap().keySet());
    }

    @Test
    void columnAccessIsBoundedByTheRowCellCount() {
        // TableSchema's public constructor keeps the caller's list, so the schema can grow under a row.
        var columns = new ArrayList<ColumnSchema>();
        columns.add(new ColumnSchema("c0", ColumnValueType.STRING));
        var schema = new TableSchema(columns, true, false, null, null);
        var writer = new ProtoWireRowWriter(1);
        writer.writeBytes(0, ColumnValueType.STRING, ByteString.copyFromUtf8("value"));
        var row = ProtoWireRowDecoder.decode(writer.finish(), schema);

        columns.add(new ColumnSchema("c1", ColumnValueType.STRING));

        assertEquals(1, row.cellCount());
        assertEquals(ByteString.copyFromUtf8("value"), row.bytes(0));
        assertThrows(IllegalArgumentException.class, () -> row.bytes(1));
        assertThrows(IllegalArgumentException.class, () -> row.type(1));
        assertThrows(IllegalArgumentException.class, () -> row.encodedCellLengthForOutput(1));
    }

    @Test
    void rejectsCellCountsThatDoNotMatchSchema() {
        var schema = TableSchema.builder().addValue("c0", TiType.uint64())
                .addValue("c1", TiType.uint64()).build();
        for (int count : new int[] {0, 1, 3}) {
            var writer = new ProtoWireRowWriter(count);
            for (int id = 0; id < count; id++) {
                writer.writeNull(id);
            }
            var bytes = writer.finish();
            assertThrows(IllegalStateException.class, () -> ProtoWireRowDecoder.decode(bytes, schema));
            assertThrows(IllegalArgumentException.class,
                    () -> new ProtoWireRowWriter(count).finishTrusted(schema));
        }
    }

    @Test
    void checkedVarintsPreserveOverlongEncodingsAndRejectOverflow() {
        for (var type : new ColumnValueType[] {ColumnValueType.INT64, ColumnValueType.UINT64}) {
            var schema = TableSchema.builder()
                    .addValue("v", type == ColumnValueType.INT64 ? TiType.int64() : TiType.uint64()).build();
            for (int length = 1; length <= 10; length++) {
                byte[] data = singleCellRow(type, overlongZero(length));
                var bytes = ByteString.copyFrom(data);
                var row = ProtoWireRowDecoder.decode(bytes, schema);
                assertEquals(0, type == ColumnValueType.INT64 ? row.getInt64(0) : row.getUint64Bits(0));
                var copy = new ProtoWireRowWriter(1);
                copy.writeFrom(0, row, 0);
                assertEquals(bytes, copy.finish());

                // Setting the continuation bit of the last byte makes the varint longer than the input.
                // A ten-byte varint has no room for more bytes, so it becomes invalid instead.
                data[data.length - 1] = (byte) 0x80;
                var broken = ByteString.copyFrom(data);
                String expected = length == 10 ? INVALID_VARINT : TRUNCATED;
                assertMalformed(expected, broken, schema);
                var schemaUnboundError = assertThrows(IllegalArgumentException.class,
                        () -> ProtoWireRowDecoder.decodeSchemaUnbound(broken));
                assertTrue(schemaUnboundError.getMessage().contains(expected), schemaUnboundError.getMessage());
            }
            byte[] tenthByteOverflow = overlongZero(10);
            tenthByteOverflow[9] = 2;
            var overflow = ByteString.copyFrom(singleCellRow(type, tenthByteOverflow));
            assertMalformed(INVALID_VARINT, overflow, schema);
            assertThrows(IllegalArgumentException.class, () -> ProtoWireRowDecoder.decodeSchemaUnbound(overflow));
        }
    }

    /**
     * The scan decodes a multi-byte varint with an unrolled fast path when at least nine more
     * bytes follow its first byte, and validates it byte by byte otherwise. A trailing 128-byte
     * string ({@code padded}) forces the fast path; a single cell mostly takes the slow path.
     */
    @Test
    void readsEveryScalarWidthOnBothVarintPaths() {
        for (var type : new ColumnValueType[] {ColumnValueType.INT64, ColumnValueType.UINT64}) {
            for (int bit = 0; bit < Long.SIZE; bit++) {
                for (long value : new long[] {(1L << bit) - 1, ~((1L << bit) - 1)}) {
                    for (boolean padded : new boolean[] {false, true}) {
                        var schemaBuilder = TableSchema.builder()
                                .addValue("v", type == ColumnValueType.INT64 ? TiType.int64() : TiType.uint64());
                        var writer = new ProtoWireRowWriter(padded ? 2 : 1);
                        if (type == ColumnValueType.INT64) {
                            writer.writeInt64(0, value);
                        } else {
                            writer.writeUint64Bits(0, value);
                        }
                        if (padded) {
                            schemaBuilder.addValue("padding", TiType.string());
                            writer.writeBytes(1, ColumnValueType.STRING, ByteString.copyFrom(new byte[128]));
                        }
                        var bytes = writer.finish();
                        var row = ProtoWireRowDecoder.decode(bytes, schemaBuilder.build());
                        assertEquals(value, type == ColumnValueType.INT64 ? row.getInt64(0) : row.getUint64Bits(0));
                        if (padded) {
                            assertEquals(ByteString.copyFrom(new byte[128]), row.bytes(1));
                        }
                        assertSame(bytes, row.wireBytes());
                    }
                }
            }
        }
    }

    /**
     * Only the unrolled fast path can reach a tenth varint byte: the slow path runs when fewer
     * than nine bytes follow the first one, and reports truncation instead.
     */
    @Test
    void rejectsEveryInvalidTenthVarintByte() {
        var schema = TableSchema.builder().addValue("v", TiType.uint64()).build();
        var paddedSchema = TableSchema.builder().addValue("v", TiType.uint64())
                .addValue("padding", TiType.string()).build();
        byte[] value = overlongZero(10);
        for (int last = 2; last <= 0xff; last++) {
            value[9] = (byte) last;
            assertMalformed(INVALID_VARINT, ByteString.copyFrom(singleCellRow(ColumnValueType.UINT64, value)), schema);
            // The same invalid varint followed by another cell.
            byte[] padded = Arrays.copyOf(singleCellRow(ColumnValueType.UINT64, value), 18);
            padded[1] = 2;
            padded[14] = 1;
            padded[15] = (byte) ColumnValueType.STRING.getValue();
            padded[16] = 1;
            assertMalformed(INVALID_VARINT, ByteString.copyFrom(padded), paddedSchema);
        }
    }

    @Test
    void readsAndCopiesLongVarintsAcrossRopeAndSliceBoundaries() {
        var schemaBuilder = TableSchema.builder();
        var writer = new ProtoWireRowWriter(100);
        for (int id = 0; id < 100; id++) {
            schemaBuilder.addValue("c" + id, TiType.int64());
            writer.writeInt64(id, Long.MIN_VALUE + id);
        }
        var schema = schemaBuilder.build();
        byte[] data = writer.finish().toByteArray();
        var rope = ByteString.copyFrom(data, 0, 59)
                .concat(ByteString.copyFrom(data, 59, data.length - 59));
        byte[] padded = new byte[data.length + 17];
        System.arraycopy(data, 0, padded, 17, data.length);
        var slice = ByteString.copyFrom(padded).substring(17);
        for (var bytes : List.of(rope, slice)) {
            var row = ProtoWireRowDecoder.decode(bytes, schema);
            var copy = new ProtoWireRowWriter(100);
            for (int id = 0; id < 100; id++) {
                assertEquals(Long.MIN_VALUE + id, row.getInt64(id));
                copy.writeFrom(id, row, id);
            }
            assertSame(bytes, row.wireBytes());
            assertEquals(bytes, copy.finish());
        }
    }

    @Test
    void schemaUnboundRowsRetainBytesAndWireOrderWithoutDecodedCells() {
        var bytes = ByteString.copyFrom(new byte[] {0, 2, 3, 4, 7, 1, 4, 8});
        var row = ProtoWireRowDecoder.decodeSchemaUnbound(bytes);

        assertSame(bytes, row.wireBytes());
        assertFalse(row.isSchemaBound());
        assertEquals(0, row.schema().getColumnsCount());
        assertEquals(2, row.cellCount());
        row.validateForOutput();
        assertThrows(UnsupportedOperationException.class, () -> row.getUint64Bits(3));
        assertThrows(UnsupportedOperationException.class, () -> row.writeTo(YTree.builder()));
        assertThrows(UnsupportedOperationException.class, () -> row.encodedCellLengthForOutput(0));
        assertNull(row.builtCells());
        assertThrows(IllegalStateException.class,
                () -> ProtoWireRowDecoder.decodeSchemaUnbound(bytes.concat(ByteString.copyFrom(new byte[] {0}))));
        assertThrows(IllegalArgumentException.class,
                () -> ProtoWireRowDecoder.decodeSchemaUnbound(bytes.substring(0, bytes.size() - 1)));
    }

    @Test
    void emptySchemaBoundRowCanBeInspected() {
        var row = ProtoWireRowDecoder.decode(ByteString.copyFrom(new byte[] {0, 0}), TableSchema.builder().build());
        assertTrue(row.isSchemaBound());
        assertEquals(0, row.cellCount());
        var tree = YTree.builder();
        row.writeTo(tree);
        assertTrue(tree.build().asMap().isEmpty());
    }

    @Test
    void decoderBuildsCellsEagerlyWithOneSlotPerColumn() {
        var schemaBuilder = TableSchema.builder();
        int count = 1024;
        var writer = new ProtoWireRowWriter(count);
        for (int i = 0; i < count; i++) {
            schemaBuilder.addValue("c" + i, TiType.uint64());
            writer.writeUint64Bits(i, 42);
        }
        var bytes = writer.finish();
        var row = ProtoWireRowDecoder.decode(bytes, schemaBuilder.build());

        assertSame(bytes, row.wireBytes());
        row.validateForOutput();
        assertEquals(count, row.cellCount());
        var cells = row.builtCells();
        assertNotNull(cells);
        assertEquals(count, row.decodedCellsCapacity());
        assertEquals(42, row.getUint64Bits(0));
        assertEquals(42, row.getUint64Bits(count - 1));
        assertSame(cells, row.builtCells());
    }

    @Test
    void trustedWriterDecodesCellsOnlyOnColumnAccess() {
        var schema = TableSchema.builder()
                .addValue("number", TiType.int64())
                .addValue("text", TiType.string())
                .build();
        var writer = new ProtoWireRowWriter(2);
        writer.writeInt64(0, -42);
        writer.writeBytes(1, ColumnValueType.STRING, ByteString.copyFromUtf8("value"));
        var row = writer.finishTrusted(schema);

        row.validateForOutput();
        assertTrue(row.isSchemaBound());
        assertEquals(2, row.cellCount());
        assertNull(row.builtCells());
        assertEquals(-1, row.decodedCellsCapacity());
        var bytes = row.wireBytes();
        assertEquals(-42, row.getInt64(0));
        var cells = row.builtCells();
        assertNotNull(cells);
        assertEquals(2, row.decodedCellsCapacity());
        assertEquals("value", row.bytes(1).toStringUtf8());
        assertSame(cells, row.builtCells());
        assertSame(bytes, row.wireBytes());
    }

    @Test
    void trustedRowReportsSchemaMismatchOnFirstAccess() {
        var schema = TableSchema.builder().addValue("number", TiType.int64()).build();
        var writer = new ProtoWireRowWriter(1);
        writer.writeBytes(0, ColumnValueType.STRING, ByteString.copyFromUtf8("not a number"));
        var row = writer.finishTrusted(schema);

        var error = assertThrows(IllegalStateException.class, () -> row.getInt64(0));
        assertTrue(error.getMessage().contains("does not match schema"), error.getMessage());
    }

    @Test
    void failedLazyDecodeNeverPublishesPartialCells() {
        var schema = TableSchema.builder()
                .addValue("first", TiType.uint64())
                .addValue("second", TiType.uint64())
                .build();
        var writer = new ProtoWireRowWriter(2);
        writer.writeUint64Bits(0, 42);
        writer.writeBytes(1, ColumnValueType.STRING, ByteString.copyFromUtf8("wrong type"));
        var row = writer.finishTrusted(schema);
        var bytes = row.wireBytes();

        assertThrows(InvalidRowException.class, () -> row.getUint64Bits(0));
        assertNull(row.builtCells());
        assertThrows(InvalidRowException.class, () -> row.getUint64Bits(1));
        assertNull(row.builtCells());
        assertSame(bytes, row.wireBytes());
    }

    @Test
    void trustedRowPublishesCompleteCellsToConcurrentReaders() throws Exception {
        var readers = Executors.newFixedThreadPool(8);
        try {
            for (int count : new int[] {1, 3, 128}) {
                var builder = TableSchema.builder();
                for (int c = 0; c < count; c++) {
                    builder.addValue("c" + c, c % 2 == 0 ? TiType.uint64() : TiType.string());
                }
                var schema = builder.build();
                for (int attempt = 0; attempt < 10; attempt++) {
                    var writer = new ProtoWireRowWriter(count);
                    for (int c = 0; c < count; c++) {
                        if (c % 2 == 0) {
                            writer.writeUint64Bits(c, Long.MIN_VALUE + c);
                        } else {
                            writer.writeBytes(c, ColumnValueType.STRING, ByteString.copyFromUtf8("value" + c));
                        }
                    }
                    var row = writer.finishTrusted(schema);
                    var ready = new CountDownLatch(8);
                    var start = new CountDownLatch(1);
                    var results = new ArrayList<Future<Object>>();
                    for (int thread = 0; thread < 8; thread++) {
                        results.add(readers.submit(() -> {
                            ready.countDown();
                            assertTrue(start.await(10, TimeUnit.SECONDS));
                            for (int c = 0; c < count; c++) {
                                if (c % 2 == 0) {
                                    assertEquals(Long.MIN_VALUE + c, row.getUint64Bits(c));
                                } else {
                                    assertEquals("value" + c, row.bytes(c).toStringUtf8());
                                }
                            }
                            return row.builtCells();
                        }));
                    }
                    assertTrue(ready.await(10, TimeUnit.SECONDS));
                    start.countDown();
                    var cells = results.get(0).get(10, TimeUnit.SECONDS);
                    assertNotNull(cells);
                    for (var result : results) {
                        assertSame(cells, result.get(10, TimeUnit.SECONDS));
                    }
                }
            }
        } finally {
            readers.shutdownNow();
            assertTrue(readers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static TableSchema schema() {
        return TableSchema.builder()
                .addValue("signed", TiType.int64())
                .addValue("unsigned", TiType.uint64())
                .addValue("ratio", TiType.doubleType())
                .addValue("enabled", TiType.bool())
                .addValue("name", TiType.string())
                .build();
    }

    @Test
    void readsTypedValuesAndImmutableByteSlice() {
        var writer = new ProtoWireRowWriter(5);
        writer.writeInt64(0, -42);
        writer.writeUint64Bits(1, -1L);
        writer.writeDouble(2, -0.0);
        writer.writeBoolean(3, true);
        writer.writeBytes(4, ColumnValueType.STRING, ByteString.copyFromUtf8("flow"));

        ProtoWireRow row = ProtoWireRowDecoder.decode(writer.finish(), schema());

        assertEquals(5, row.cellCount());
        assertEquals(-42, row.getInt64(0));
        assertEquals(-1L, row.getUint64Bits(1));
        assertEquals(
                Double.doubleToRawLongBits(-0.0),
                Double.doubleToRawLongBits(row.getDouble(2))
        );
        assertTrue(row.getBoolean(3));
        assertEquals("flow", row.bytes(4).toStringUtf8());
        assertEquals(ByteString.copyFromUtf8("flow"), row.bytes(4));
        assertEquals(4, row.valueLength(4));
    }

    @Test
    void accessorsRejectWrongTypesAndUnknownColumns() {
        var writer = new ProtoWireRowWriter(5);
        writer.writeInt64(0, -42);
        writer.writeUint64Bits(1, 1);
        writer.writeDouble(2, 1.5);
        writer.writeBoolean(3, true);
        writer.writeNull(4);
        ProtoWireRow row = ProtoWireRowDecoder.decode(writer.finish(), schema());

        assertWrongType("Expected: UINT64, Actual: INT64", () -> row.getUint64Bits(0));
        assertWrongType("Expected: INT64, Actual: UINT64", () -> row.getInt64(1));
        assertWrongType("Expected: BOOLEAN, Actual: DOUBLE", () -> row.getBoolean(2));
        assertWrongType("Expected: DOUBLE, Actual: BOOLEAN", () -> row.getDouble(3));
        assertWrongType("Expected: INT64, Actual: NULL", () -> row.getInt64(4));
        assertWrongType("INT64 is not a string-like value", () -> row.bytes(0));
        assertWrongType("NULL is not a string-like value", () -> row.valueLength(4));
        assertWrongType("DOUBLE is not a string-like value", () -> row.bytes(2));
        assertWrongType("BOOLEAN is not a string-like value", () -> row.valueLength(3));

        var text = ProtoWireRowDecoder.decode(ByteString.copyFrom(
                singleCellRow(ColumnValueType.STRING, (byte) 2, (byte) 'o', (byte) 'k')), singleString());
        assertThrows(IllegalArgumentException.class, () -> text.bytes(1));
    }

    private static void assertWrongType(String message, Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static TableSchema singleString() {
        return TableSchema.builder().addValue("value", TiType.string()).build();
    }

    @Test
    void rejectsTruncatedHeadersAndFixedWidthValues() {
        for (byte[] data : List.of(new byte[0], new byte[] {0}, new byte[] {0, (byte) 0x81}, new byte[] {0, 1},
                new byte[] {0, 1, 0})) {
            assertMalformed(TRUNCATED, ByteString.copyFrom(data), singleString());
        }
        var doubleSchema = TableSchema.builder().addValue("value", TiType.doubleType()).build();
        assertMalformed(TRUNCATED, ByteString.copyFrom(
                singleCellRow(ColumnValueType.DOUBLE, new byte[Long.BYTES - 1])), doubleSchema);
        var booleanSchema = TableSchema.builder().addValue("value", TiType.bool()).build();
        assertMalformed(TRUNCATED, ByteString.copyFrom(singleCellRow(ColumnValueType.BOOLEAN)), booleanSchema);
    }

    @Test
    void schemaUnboundKeysAcceptUnsignedColumnIdsAtTheWireLimit() {
        var bytes = ByteString.copyFrom(new byte[] {0, 1, (byte) 0xff, (byte) 0xff, 3, 2});
        var row = ProtoWireRowDecoder.decodeSchemaUnbound(bytes);
        assertSame(bytes, row.wireBytes());
        assertEquals(1, row.cellCount());
        var invalid = ByteString.copyFrom(new byte[] {0, 1, (byte) 0x80, (byte) 0x80, 4, 2});
        assertThrows(IllegalStateException.class, () -> ProtoWireRowDecoder.decodeSchemaUnbound(invalid));
    }

    @Test
    void readsCxxCompatibleVarintTypeAndRawBoolean() {
        TableSchema schema = TableSchema.builder()
                .addValue("flag", TiType.bool())
                .addValue("sentinel", TiType.string())
                .build();
        ByteString bytes = ByteString.copyFrom(new byte[] {
                0, 2,
                0, (byte) ColumnValueType.BOOLEAN.getValue(), 2,
                1, (byte) 0xef, 0x01
        });

        ProtoWireRow row = ProtoWireRowDecoder.decode(bytes, schema);

        assertFalse(row.getBoolean(0));
        assertEquals(ColumnValueType.MAX, row.type(1));
        assertEquals(bytes, row.wireBytes());
    }

    @Test
    void rejectsDuplicateColumnId() {
        TableSchema schema = TableSchema.builder()
                .addValue("first", TiType.string())
                .addValue("second", TiType.string())
                .build();
        ByteString bytes = ByteString.copyFrom(new byte[] {
                0, 2,
                0, (byte) ColumnValueType.NULL.getValue(),
                0, (byte) ColumnValueType.NULL.getValue()
        });

        assertThrows(IllegalStateException.class, () -> ProtoWireRowDecoder.decode(bytes, schema));
    }

    @Test
    void rejectsTruncatedValue() {
        TableSchema schema = TableSchema.builder().addValue("value", TiType.string()).build();
        ByteString bytes = ByteString.copyFrom(new byte[] {
                0, 1,
                0, (byte) ColumnValueType.STRING.getValue(), 2, 'x'
        });

        assertMalformed(TRUNCATED, bytes, schema);
    }

    @Test
    void acceptsStringLikeInputAboveWriterLimit() {
        int valueLength = 16 * 1024 * 1024 + 1;
        ByteString value = ByteString.copyFrom(new byte[valueLength]);

        for (ColumnValueType type : new ColumnValueType[] {
                ColumnValueType.STRING,
                ColumnValueType.ANY,
                ColumnValueType.COMPOSITE
        }) {
            TableSchema schema = switch (type) {
                case STRING -> TableSchema.builder().addValue("value", TiType.string()).build();
                case ANY -> TableSchema.builder().addValue("value", TiType.yson()).build();
                case COMPOSITE -> TableSchema.builder()
                        .addValue("value", TiType.list(TiType.string()))
                        .build();
                default -> throw new AssertionError(type);
            };
            ByteString header = ByteString.copyFrom(new byte[] {
                    0, 1,
                    0, (byte) type.getValue(),
                    (byte) 0x81, (byte) 0x80, (byte) 0x80, 0x08
            });

            ProtoWireRow row = ProtoWireRowDecoder.decode(header.concat(value), schema);

            assertEquals(valueLength, row.valueLength(0));
            assertEquals(0, row.bytes(0).byteAt(valueLength - 1));
            assertThrows(IllegalStateException.class, row::validateForOutput);
            assertThrows(IllegalStateException.class, () -> row.encodedCellLengthForOutput(0));
            var schemaUnbound = ProtoWireRowDecoder.decodeSchemaUnbound(row.wireBytes());
            assertThrows(IllegalStateException.class, schemaUnbound::validateForOutput);

            var outputWriter = new ProtoWireRowWriter(1);
            assertThrows(
                    IllegalStateException.class,
                    () -> outputWriter.writeFrom(0, row, 0)
            );
        }
    }

    @Test
    void rejectsStringLengthAboveIntegerRange() {
        TableSchema schema = TableSchema.builder().addValue("value", TiType.string()).build();
        ByteString bytes = ByteString.copyFrom(new byte[] {
                0, 1,
                0, (byte) ColumnValueType.STRING.getValue(),
                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x08
        });

        assertThrows(IllegalStateException.class, () -> ProtoWireRowDecoder.decode(bytes, schema));
    }

    @Test
    void rejectsNonUint32StringLength() {
        TableSchema schema = TableSchema.builder().addValue("value", TiType.string()).build();
        ByteString bytes = ByteString.copyFrom(new byte[] {
                0, 1,
                0, (byte) ColumnValueType.STRING.getValue(),
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01
        });

        assertMalformed(INVALID_VARINT, bytes, schema);
    }

    @Test
    void rejectsNullRowMarker() {
        ByteString bytes = ByteString.copyFrom(new byte[] {
                0,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0f
        });

        assertThrows(
                IllegalStateException.class,
                () -> ProtoWireRowDecoder.decode(bytes, TableSchema.builder().build())
        );
    }

    @Test
    void rejectsExcessiveCellCount() {
        ByteString bytes = ByteString.copyFrom(new byte[] {0, (byte) 0x81, 0x08});

        assertThrows(
                IllegalStateException.class,
                () -> ProtoWireRowDecoder.decode(bytes, TableSchema.builder().build())
        );
    }

    @Test
    void rejectsTrailingBytes() {
        ByteString bytes = ByteString.copyFrom(new byte[] {0, 0, 42});

        assertThrows(
                IllegalStateException.class,
                () -> ProtoWireRowDecoder.decode(bytes, TableSchema.builder().build())
        );
    }
}
