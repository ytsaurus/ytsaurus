package tech.ytsaurus.flow.internal.row;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.client.rows.WireProtocol;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("checkstyle:LineLength")
class ProtoWireRowWriterTest {
    private static final int MAX_VALUE_LENGTH = 16 * 1024 * 1024;
    private static final int EVERY_KIND_COUNT = 12;

    @Test
    void copyPreservesNullsAndEncodedCells() {
        var schema = TableSchema.builder()
                .addValue("null", TiType.uint64())
                .addValue("value", TiType.uint64())
                .build();
        var row = ProtoWireRow.fromWire(
                ByteString.copyFrom(new byte[] {0, 2, 0, 2, 1, 4, (byte) 0x81, 0}), schema);

        assertEquals(2, row.encodedCellLengthForOutput(0));
        assertEquals(4, row.encodedCellLengthForOutput(1));
        assertThrows(IllegalArgumentException.class, () -> row.encodedCellLengthForOutput(2));
        var writer = new ProtoWireRowWriter(2);
        writer.writeFrom(0, row, 0);
        writer.writeFrom(1, row, 1);
        assertArrayEquals(new byte[] {0, 2, 0, 2, 1, 4, (byte) 0x81, 0}, writer.finish().toByteArray());
    }

    @Test
    void writesExactCompactBytes() {
        var writer = new ProtoWireRowWriter(6);
        writer.writeInt64(0, -1);
        writer.writeUint64Bits(1, 150);
        writer.writeDouble(2, 1.0);
        writer.writeBoolean(3, true);
        writer.writeBytes(4, ColumnValueType.STRING, ByteString.copyFromUtf8("ok"));
        writer.writeNull(5);

        assertArrayEquals(new byte[] {
                0, 6,
                0, 3, 1,
                1, 4, (byte) 0x96, 1,
                2, 5, 0, 0, 0, 0, 0, 0, (byte) 0xf0, 0x3f,
                3, 6, 1,
                4, 0x10, 2, 'o', 'k',
                5, 2
        }, writer.finish().toByteArray());
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, -1L, 42L, -42L, Long.MAX_VALUE, Long.MIN_VALUE})
    void int64ExtremesRoundTrip(long value) {
        int exactSize = ProtoWireRowWriter.encodedRowHeaderSize(1)
                + ProtoWireRowWriter.encodedCellSize(0, ColumnValueType.INT64, value, null);
        var writer = new ProtoWireRowWriter(1, exactSize);
        writer.writeInt64(0, value);

        ByteString bytes = writer.finish();
        ProtoWireRow row = ProtoWireRow.fromWire(bytes, singleColumnSchema(ColumnValueType.INT64));

        assertEquals(value, row.getInt64(0));
        assertEquals(exactSize, bytes.size());
        assertEquals(exactSize, writer.retainedCapacity());
    }

    @ParameterizedTest
    @MethodSource("uint64Encodings")
    void uint64UsesExactUnsignedVarint(long value, byte[] encodedValue) {
        int exactSize = ProtoWireRowWriter.encodedRowHeaderSize(1)
                + ProtoWireRowWriter.encodedCellSize(0, ColumnValueType.UINT64, value, null);
        var writer = new ProtoWireRowWriter(1, exactSize);
        writer.writeUint64Bits(0, value);
        byte[] expected = new byte[4 + encodedValue.length];
        expected[0] = 0;
        expected[1] = 1;
        expected[2] = 0;
        expected[3] = (byte) ColumnValueType.UINT64.getValue();
        System.arraycopy(encodedValue, 0, expected, 4, encodedValue.length);

        ByteString bytes = writer.finish();
        ProtoWireRow row = ProtoWireRow.fromWire(bytes, singleColumnSchema(ColumnValueType.UINT64));

        assertArrayEquals(expected, bytes.toByteArray());
        assertEquals(expected.length, exactSize);
        assertEquals(exactSize, writer.retainedCapacity());
        assertEquals(value, row.getUint64Bits(0));
    }

    @ParameterizedTest
    @MethodSource("doubleValues")
    void doubleRawBitsRoundTrip(double value) {
        var writer = new ProtoWireRowWriter(1);
        writer.writeDouble(0, value);

        ProtoWireRow row = ProtoWireRow.fromWire(writer.finish(), singleColumnSchema(ColumnValueType.DOUBLE));

        assertEquals(Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(row.getDouble(0)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void booleanRoundTrip(boolean value) {
        var writer = new ProtoWireRowWriter(1);
        writer.writeBoolean(0, value);

        ProtoWireRow row = ProtoWireRow.fromWire(writer.finish(), singleColumnSchema(ColumnValueType.BOOLEAN));

        assertEquals(value, row.getBoolean(0));
    }

    @ParameterizedTest
    @MethodSource("bytePayloads")
    void allStringLikeTypesRoundTrip(String label, byte[] payload) {
        for (ColumnValueType type : new ColumnValueType[] {
                ColumnValueType.STRING,
                ColumnValueType.ANY,
                ColumnValueType.COMPOSITE
        }) {
            var writer = new ProtoWireRowWriter(1);
            writer.writeBytes(0, type, ByteString.copyFrom(payload));

            ProtoWireRow row = ProtoWireRow.fromWire(writer.finish(), singleColumnSchema(type));

            assertEquals(type, row.type(0), label);
            assertArrayEquals(payload, row.bytes(0).toByteArray(), label);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ColumnValueType.class, names = {"STRING", "ANY", "COMPOSITE"})
    void acceptsStringLikeValueOfExactly16MiB(ColumnValueType type) {
        ByteString value = ByteString.copyFrom(new byte[MAX_VALUE_LENGTH]);
        var writer = new ProtoWireRowWriter(1);

        writer.writeBytes(0, type, value);
        ByteString bytes = writer.finish();
        ProtoWireRow row = ProtoWireRow.fromWire(bytes, singleColumnSchema(type));

        assertEquals(MAX_VALUE_LENGTH, row.valueLength(0));
        assertTrue(bytes.size() > MAX_VALUE_LENGTH);
    }

    @ParameterizedTest
    @EnumSource(value = ColumnValueType.class, names = {"STRING", "ANY", "COMPOSITE"})
    void rejectsStringLikeValueLargerThan16MiB(ColumnValueType type) {
        ByteString value = ByteString.copyFrom(new byte[MAX_VALUE_LENGTH + 1]);
        var writer = new ProtoWireRowWriter(1);

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> writer.writeBytes(0, type, value)
        );

        assertTrue(error.getMessage().contains(type.toString()));
    }

    @ParameterizedTest
    @EnumSource(value = ColumnValueType.class, names = {"NULL", "MIN", "MAX", "THE_BOTTOM"})
    void sentinelsContainOnlyHeader(ColumnValueType type) {
        int exactSize = ProtoWireRowWriter.encodedRowHeaderSize(1)
                + ProtoWireRowWriter.encodedCellSize(0, type, 0, null);
        var writer = new ProtoWireRowWriter(1, exactSize);
        writer.writeEntity(0, type);

        ByteString bytes = writer.finish();
        ProtoWireRow row = ProtoWireRow.fromWire(bytes, singleColumnSchema(ColumnValueType.STRING));

        byte[] expected = type == ColumnValueType.MAX
                ? new byte[] {0, 1, 0, (byte) 0xef, 0x01}
                : new byte[] {0, 1, 0, (byte) type.getValue()};
        assertArrayEquals(expected, bytes.toByteArray());
        assertEquals(exactSize, bytes.size());
        assertEquals(exactSize, writer.retainedCapacity());
        assertEquals(type, row.type(0));
    }

    @Test
    void mixedRowRoundTrip() {
        TableSchema schema = TableSchema.builder()
                .addValue("null", TiType.string())
                .addValue("signed", TiType.int64())
                .addValue("unsigned", TiType.uint64())
                .addValue("double", TiType.doubleType())
                .addValue("boolean", TiType.bool())
                .add(new ColumnSchema("string", ColumnValueType.STRING))
                .add(new ColumnSchema("any", ColumnValueType.ANY))
                .add(new ColumnSchema("composite", TiType.list(TiType.int64())))
                .build();
        var writer = new ProtoWireRowWriter(8);
        writer.writeNull(0);
        writer.writeInt64(1, -123);
        writer.writeUint64Bits(2, Long.MIN_VALUE);
        writer.writeDouble(3, Math.PI);
        writer.writeBoolean(4, true);
        writer.writeBytes(5, ColumnValueType.STRING, ByteString.copyFromUtf8("str"));
        writer.writeBytes(6, ColumnValueType.ANY, ByteString.copyFromUtf8("{a=1}"));
        writer.writeBytes(7, ColumnValueType.COMPOSITE, ByteString.copyFromUtf8("[1;2]"));

        ProtoWireRow row = ProtoWireRow.fromWire(writer.finish(), schema);

        assertTrue(row.isNull(0));
        assertEquals(-123, row.getInt64(1));
        assertEquals(Long.MIN_VALUE, row.getUint64Bits(2));
        assertEquals(Double.doubleToRawLongBits(Math.PI), Double.doubleToRawLongBits(row.getDouble(3)));
        assertTrue(row.getBoolean(4));
        assertEquals("str", row.bytes(5).toStringUtf8());
        assertArrayEquals("{a=1}".getBytes(StandardCharsets.UTF_8), row.bytes(6).toByteArray());
        assertArrayEquals("[1;2]".getBytes(StandardCharsets.UTF_8), row.bytes(7).toByteArray());
    }

    @Test
    void copiesUnchangedCellsFromView() {
        TableSchema schema = TableSchema.builder()
                .addValue("number", TiType.uint64())
                .addValue("text", TiType.string())
                .build();
        var sourceWriter = new ProtoWireRowWriter(2);
        sourceWriter.writeUint64Bits(0, -1L);
        sourceWriter.writeBytes(1, ColumnValueType.STRING, ByteString.copyFromUtf8("value"));
        ProtoWireRow source = ProtoWireRow.fromWire(sourceWriter.finish(), schema);

        var writer = new ProtoWireRowWriter(2);
        writer.writeFrom(0, source, 0);
        writer.writeFrom(1, source, 1);

        assertEquals(source.wireBytes(), writer.finish());
    }

    @Test
    void copiesUnchangedCellBytesWithoutCanonicalizingThem() {
        TableSchema schema = TableSchema.builder()
                .addValue("flag", TiType.bool())
                .addValue("text", TiType.string())
                .build();
        ByteString sourceBytes = ByteString.copyFrom(new byte[] {
                0, 2,
                0, (byte) ColumnValueType.BOOLEAN.getValue(), 2,
                1, (byte) ColumnValueType.STRING.getValue(), 1, 'x'
        });
        ProtoWireRow source = ProtoWireRow.fromWire(sourceBytes, schema);

        var writer = new ProtoWireRowWriter(2);
        writer.writeFrom(0, source, 0);
        writer.writeBytes(1, ColumnValueType.STRING, ByteString.copyFromUtf8("y"));

        assertArrayEquals(new byte[] {
                0, 2,
                0, (byte) ColumnValueType.BOOLEAN.getValue(), 2,
                1, (byte) ColumnValueType.STRING.getValue(), 1, 'y'
        }, writer.finish().toByteArray());
    }

    /**
     * Writes one cell of every kind; {@code firstId} shifts all column ids.
     */
    private static void writeEveryKind(ProtoWireRowWriter writer, int firstId) {
        writer.writeInt64(firstId, -123);
        writer.writeUint64Bits(firstId + 1, -1L);
        writer.writeDouble(firstId + 2, -0.0);
        writer.writeDouble(firstId + 3, Double.NaN);
        writer.writeBoolean(firstId + 4, true);
        writer.writeBytes(firstId + 5, ColumnValueType.STRING, ByteString.copyFromUtf8("str"));
        writer.writeBytes(firstId + 6, ColumnValueType.ANY, ByteString.copyFromUtf8("{a=1}"));
        writer.writeBytes(firstId + 7, ColumnValueType.COMPOSITE, ByteString.copyFromUtf8("[1;2]"));
        writer.writeNull(firstId + 8);
        writer.writeEntity(firstId + 9, ColumnValueType.MIN);
        writer.writeEntity(firstId + 10, ColumnValueType.MAX);
        writer.writeEntity(firstId + 11, ColumnValueType.THE_BOTTOM);
    }

    private static TableSchema everyKindSchema() {
        return TableSchema.builder()
                .addValue("signed", TiType.int64())
                .addValue("unsigned", TiType.uint64())
                .addValue("zero", TiType.doubleType())
                .addValue("nan", TiType.doubleType())
                .addValue("flag", TiType.bool())
                .add(new ColumnSchema("string", ColumnValueType.STRING))
                .add(new ColumnSchema("any", ColumnValueType.ANY))
                .add(new ColumnSchema("composite", TiType.list(TiType.int64())))
                .addValue("null", TiType.string())
                .addValue("min", TiType.string())
                .addValue("max", TiType.string())
                .addValue("bottom", TiType.string())
                .build();
    }

    @Test
    void reencodesCellsCopiedIntoAnotherColumn() {
        var sourceWriter = new ProtoWireRowWriter(EVERY_KIND_COUNT);
        writeEveryKind(sourceWriter, 0);
        var source = ProtoWireRow.fromWire(sourceWriter.finish(), everyKindSchema());

        var expected = new ProtoWireRowWriter(EVERY_KIND_COUNT + 1);
        expected.writeNull(0);
        writeEveryKind(expected, 1);
        var shifted = new ProtoWireRowWriter(EVERY_KIND_COUNT + 1);
        shifted.writeNull(0);
        for (int id = 0; id < EVERY_KIND_COUNT; id++) {
            shifted.writeFrom(id + 1, source, id);
        }

        assertEquals(expected.finish(), shifted.finish());
    }

    @Test
    void reencodesCellsFromOtherRowViews() {
        var sourceWriter = new ProtoWireRowWriter(EVERY_KIND_COUNT);
        writeEveryKind(sourceWriter, 0);
        var source = ProtoWireRow.fromWire(sourceWriter.finish(), everyKindSchema());
        RowView view = delegatingView(source);

        var copy = new ProtoWireRowWriter(EVERY_KIND_COUNT);
        for (int id = 0; id < EVERY_KIND_COUNT; id++) {
            copy.writeFrom(id, view, id);
        }

        assertEquals(source.wireBytes(), copy.finish());
    }

    @Test
    void reencodingCanonicalizesBooleansThatVerbatimCopiesKeep() {
        var schema = TableSchema.builder().addValue("flag", TiType.bool()).addValue("other", TiType.bool()).build();
        var source = ProtoWireRow.fromWire(ByteString.copyFrom(new byte[] {
                0, 2,
                0, (byte) ColumnValueType.BOOLEAN.getValue(), 2,
                1, (byte) ColumnValueType.BOOLEAN.getValue(), 1
        }), schema);

        var verbatim = new ProtoWireRowWriter(2);
        verbatim.writeFrom(0, source, 0);
        verbatim.writeFrom(1, source, 1);
        var reencoded = new ProtoWireRowWriter(2);
        reencoded.writeFrom(0, source, 1);
        reencoded.writeFrom(1, source, 0);

        assertEquals(source.wireBytes(), verbatim.finish());
        assertArrayEquals(new byte[] {
                0, 2,
                0, (byte) ColumnValueType.BOOLEAN.getValue(), 1,
                1, (byte) ColumnValueType.BOOLEAN.getValue(), 0
        }, reencoded.finish().toByteArray());
    }

    @Test
    void copiesCellRangesVerbatim() {
        var sourceWriter = new ProtoWireRowWriter(EVERY_KIND_COUNT);
        writeEveryKind(sourceWriter, 0);
        var source = ProtoWireRow.fromWire(sourceWriter.finish(), everyKindSchema());

        var copy = new ProtoWireRowWriter(EVERY_KIND_COUNT);
        copy.writeInt64(0, -123);
        copy.writeFrom(source, 1, 1);
        copy.writeFrom(source, 1, 5);
        copy.writeFrom(5, source, 5);
        copy.writeFrom(source, 6, EVERY_KIND_COUNT);

        assertEquals(source.wireBytes(), copy.finish());
    }

    @Test
    void rangeCopyChecksPositionsBoundsAndOutputLimits() {
        var sourceWriter = new ProtoWireRowWriter(EVERY_KIND_COUNT);
        writeEveryKind(sourceWriter, 0);
        var source = ProtoWireRow.fromWire(sourceWriter.finish(), everyKindSchema());

        assertThrows(IllegalStateException.class, () -> new ProtoWireRowWriter(EVERY_KIND_COUNT).writeFrom(source, 1, 3));
        assertThrows(IllegalStateException.class, () -> new ProtoWireRowWriter(2).writeFrom(source, 0, 3));
        assertThrows(IndexOutOfBoundsException.class,
                () -> new ProtoWireRowWriter(EVERY_KIND_COUNT).writeFrom(source, 0, EVERY_KIND_COUNT + 1));
        assertThrows(IndexOutOfBoundsException.class, () -> new ProtoWireRowWriter(EVERY_KIND_COUNT).writeFrom(source, 2, 1));

        var schema = TableSchema.builder().addValue("number", TiType.int64()).addValue("text", TiType.string()).build();
        var header = ByteString.copyFrom(new byte[] {
                0, 2,
                0, (byte) ColumnValueType.INT64.getValue(), 0,
                1, (byte) ColumnValueType.STRING.getValue(), (byte) 0x81, (byte) 0x80, (byte) 0x80, 0x08
        });
        var oversized = ProtoWireRow.fromWire(header.concat(ByteString.copyFrom(new byte[MAX_VALUE_LENGTH + 1])), schema);
        var writer = new ProtoWireRowWriter(2);
        writer.writeFrom(oversized, 0, 1);
        assertThrows(IllegalStateException.class, () -> writer.writeFrom(oversized, 1, 2));
    }

    private static RowView delegatingView(ProtoWireRow row) {
        return new RowView() {
            @Override
            public int cellCount() {
                return row.cellCount();
            }

            @Override
            public ColumnValueType type(int columnId) {
                return row.type(columnId);
            }

            @Override
            public long getInt64(int columnId) {
                return row.getInt64(columnId);
            }

            @Override
            public long getUint64Bits(int columnId) {
                return row.getUint64Bits(columnId);
            }

            @Override
            public double getDouble(int columnId) {
                return row.getDouble(columnId);
            }

            @Override
            public boolean getBoolean(int columnId) {
                return row.getBoolean(columnId);
            }

            @Override
            public int valueLength(int columnId) {
                return row.valueLength(columnId);
            }

            @Override
            public ByteString bytes(int columnId) {
                return row.bytes(columnId);
            }
        };
    }

    @Test
    void trimsGrosslyOverestimatedOutputBeforeUnsafeWrap() {
        var writer = new ProtoWireRowWriter(1, MAX_VALUE_LENGTH);
        writer.writeBytes(0, ColumnValueType.STRING, ByteString.copyFromUtf8("tiny"));

        ByteString bytes = writer.finish();

        assertEquals(bytes.size(), writer.retainedCapacity());
        assertTrue(writer.retainedCapacity() < 32);
    }

    @Test
    void rejectsWrongCellCount() {
        assertThrows(IllegalStateException.class, () -> new ProtoWireRowWriter(1).finish());
    }

    @Test
    void acceptsMaximumCellCount() {
        var schemaBuilder = TableSchema.builder();
        var writer = new ProtoWireRowWriter(WireProtocol.MAX_VALUES_PER_ROW);
        for (int columnId = 0; columnId < WireProtocol.MAX_VALUES_PER_ROW; columnId++) {
            schemaBuilder.addValue("column-" + columnId, TiType.string());
            writer.writeNull(columnId);
        }

        ProtoWireRow row = ProtoWireRow.fromWire(writer.finish(), schemaBuilder.build());

        assertEquals(WireProtocol.MAX_VALUES_PER_ROW, row.cellCount());
        assertTrue(row.isNull(WireProtocol.MAX_VALUES_PER_ROW - 1));
    }

    @Test
    void rejectsSkippedColumnIdsWithoutAdvancingTheWriter() {
        var writer = new ProtoWireRowWriter(2);
        assertThrows(IllegalStateException.class, () -> writer.writeNull(1));
        writer.writeNull(0);
        assertThrows(IllegalStateException.class, () -> writer.writeNull(2));
        writer.writeNull(1);
        assertArrayEquals(new byte[] {0, 2, 0, 2, 1, 2}, writer.finish().toByteArray());
    }

    @Test
    void rejectsCellCountAboveWireLimit() {
        int cellCount = WireProtocol.MAX_VALUES_PER_ROW + 1;
        assertThrows(IllegalStateException.class, () -> new ProtoWireRowWriter(cellCount));
        assertThrows(IllegalStateException.class, () -> ProtoWireRowWriter.encodedRowHeaderSize(cellCount));
    }

    @Test
    void rejectsNonIncreasingColumnIds() {
        var writer = new ProtoWireRowWriter(2);
        writer.writeNull(0);

        assertThrows(IllegalStateException.class, () -> writer.writeNull(0));
    }

    @Test
    void rejectsWritesAfterFinish() {
        var writer = new ProtoWireRowWriter(0);
        writer.finish();

        assertThrows(IllegalStateException.class, () -> writer.writeNull(0));
        assertThrows(IllegalStateException.class, writer::finish);
    }

    private static TableSchema singleColumnSchema(ColumnValueType type) {
        ColumnSchema column = type == ColumnValueType.COMPOSITE
                ? new ColumnSchema("value", TiType.list(TiType.int64()))
                : new ColumnSchema("value", type);
        return TableSchema.builder().add(column).build();
    }

    static Stream<Arguments> doubleValues() {
        return Stream.of(
                Arguments.of(0.0),
                Arguments.of(-0.0),
                Arguments.of(1.0),
                Arguments.of(-1.0),
                Arguments.of(Math.PI),
                Arguments.of(Double.MIN_VALUE),
                Arguments.of(Double.MAX_VALUE),
                Arguments.of(Double.MIN_NORMAL),
                Arguments.of(Double.POSITIVE_INFINITY),
                Arguments.of(Double.NEGATIVE_INFINITY),
                Arguments.of(Double.NaN)
        );
    }

    static Stream<Arguments> bytePayloads() {
        return Stream.of(
                Arguments.of("empty", new byte[0]),
                Arguments.of("ascii", "hello".getBytes(StandardCharsets.UTF_8)),
                Arguments.of("utf8", "Привет, мир 🚀".getBytes(StandardCharsets.UTF_8)),
                Arguments.of("zero-bytes", new byte[] {0, 0, 0, 0}),
                Arguments.of("binary", new byte[] {(byte) 0xff, 0, (byte) 0x80})
        );
    }

    static Stream<Arguments> uint64Encodings() {
        return Stream.of(
                Arguments.of(0L, new byte[] {0}),
                Arguments.of(1L, new byte[] {1}),
                Arguments.of((1L << 7) - 1, new byte[] {0x7f}),
                Arguments.of(1L << 7, new byte[] {(byte) 0x80, 0x01}),
                Arguments.of((1L << 14) - 1, new byte[] {(byte) 0xff, 0x7f}),
                Arguments.of(1L << 14, new byte[] {(byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of((1L << 21) - 1, new byte[] {(byte) 0xff, (byte) 0xff, 0x7f}),
                Arguments.of(1L << 21, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of((1L << 28) - 1, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}),
                Arguments.of(1L << 28, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of((1L << 35) - 1, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}),
                Arguments.of(1L << 35, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of((1L << 42) - 1, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}),
                Arguments.of(1L << 42, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of((1L << 49) - 1, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}),
                Arguments.of(1L << 49, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of((1L << 56) - 1, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}),
                Arguments.of(1L << 56, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of(Long.MAX_VALUE, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}),
                Arguments.of(Long.MIN_VALUE, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01}),
                Arguments.of(-1L, new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01})
        );
    }
}
