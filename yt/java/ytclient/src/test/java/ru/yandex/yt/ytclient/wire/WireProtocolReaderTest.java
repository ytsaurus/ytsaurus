package ru.yandex.yt.ytclient.wire;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;

import ru.yandex.yt.ytclient.tables.ColumnValueType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

public class WireProtocolReaderTest extends WireProtocolTest {
    private static void process(byte[] data, Consumer<WireProtocolReader> consumer) {
        WireProtocolReader reader = new WireProtocolReader(Collections.singletonList(data));
        consumer.accept(reader);
        assertFalse("reader still readable after the test", reader.readable());
    }

    @Test
    public void readUnversionedRow() {
        process(makeUnversionedRowCanonicalBlob(), reader -> {
            UnversionedRow sample = makeUnversionedRowSample();
            UnversionedRow row = reader.readUnversionedRow();
            assertThat(row, is(sample));
        });
    }

    @Test
    public void readSchemafulRow() {
        process(makeSchemafulRowCanonicalBlob(), reader -> {
            UnversionedRow sample = makeSchemafulRowSample();
            UnversionedRow row = reader.readSchemafulRow(extractSchemaData(sample, ColumnValueType.INT64));
            assertThat(row, is(sample));
        });
    }

    @Test
    public void nullBitmap() {
        // Test that schemaful reader/writer properly treats null bitmap
        byte[] blob = makeByteArray(
                // value count = 4
                0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // null bitmap = 1 << 3
                0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // id = 0, type = int64, data = 1
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // id = 1, type = int64, data = 1
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // id = 2, type = string, data = "2"
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x32, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf
                // id = 3, type = string, data = (null)
        );
        List<WireColumnSchema> blobSchema = Arrays.asList(
                new WireColumnSchema(0, ColumnValueType.INT64),
                new WireColumnSchema(1, ColumnValueType.INT64),
                new WireColumnSchema(2, ColumnValueType.STRING),
                new WireColumnSchema(3, ColumnValueType.STRING));
        UnversionedRow expected = new UnversionedRow(Arrays.asList(
                new UnversionedValue(0, ColumnValueType.INT64, false, 1L),
                new UnversionedValue(1, ColumnValueType.INT64, false, 1L),
                new UnversionedValue(2, ColumnValueType.STRING, false, new byte[]{'2'}),
                new UnversionedValue(3, ColumnValueType.NULL, false, null)));
        process(blob, reader -> {
            UnversionedRow row = reader.readSchemafulRow(blobSchema);
            assertThat(row, is(expected));
        });
    }

    @Test
    public void sentinelMinMax() {
        byte[] blob = makeByteArray(
                0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value count = 3
                0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, // id = 0, type = null
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id = 1, type = min
                0x02, 0x00, 0xef, 0x00, 0x00, 0x00, 0x00, 0x00 // id = 2, type = max
        );
        UnversionedRow expected = new UnversionedRow(Arrays.asList(
                new UnversionedValue(0, ColumnValueType.NULL, false, null),
                new UnversionedValue(1, ColumnValueType.MIN, false, null),
                new UnversionedValue(2, ColumnValueType.MAX, false, null)));
        process(blob, reader -> {
            UnversionedRow row = reader.readUnversionedRow();
            assertThat(row, is(expected));
        });
    }
}
