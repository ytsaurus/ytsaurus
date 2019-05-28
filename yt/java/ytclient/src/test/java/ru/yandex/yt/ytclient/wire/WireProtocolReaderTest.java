package ru.yandex.yt.ytclient.wire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.misc.codec.Hex;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.object.MappedRowsetDeserializer;
import ru.yandex.yt.ytclient.object.SchemafulRowDeserializer;
import ru.yandex.yt.ytclient.object.SchemafulRowsetDeserializer;
import ru.yandex.yt.ytclient.object.UnversionedRowDeserializer;
import ru.yandex.yt.ytclient.object.UnversionedRowsetDeserializer;
import ru.yandex.yt.ytclient.object.VersionedRowDeserializer;
import ru.yandex.yt.ytclient.object.VersionedRowsetDeserializer;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class WireProtocolReaderTest extends WireProtocolTest {

    private static void process(byte[] data, Consumer<WireProtocolReader> consumer) {
        WireProtocolReader reader = new WireProtocolReader(Collections.singletonList(data));
        consumer.accept(reader);
        assertFalse("reader still readable after the test", reader.readable());
    }

    private static <T> void process(List<byte[]> data, TRowsetDescriptor descriptor, Class<T> clazz,
            Consumer<T> consumer)
    {
        process(data, descriptor, clazz, consumer, WireProtocolReader::readUnversionedRow);
    }


    private static <T> void process(List<byte[]> data, TRowsetDescriptor descriptor, Class<T> clazz,
            Consumer<T> consumer, BiConsumer<WireProtocolReader, MappedRowsetDeserializer<T>> readFunction)
    {
        final WireProtocolReader reader = new WireProtocolReader(data);

        final TableSchema tableSchema = tableSchema(descriptor);

        final YTreeObjectSerializer<T> serializer =
                (YTreeObjectSerializer<T>) YTreeObjectSerializerFactory.forClass(clazz);

        final List<T> rows = new ArrayList<>();
        final MappedRowsetDeserializer<T> rowBuilder =
                MappedRowsetDeserializer.forClass(tableSchema, serializer, rows::add);

        readFunction.accept(reader, rowBuilder);
        assertFalse("reader still readable after the test", reader.readable());

        assertEquals(1, rows.size());
        consumer.accept(rows.get(0));
    }

    private static void process(RowSampleAllObject sample) {
        process(makeAttachmentsForMappedObject(sample),
                makeDescriptorForMappedObject(), RowSampleAllObject.class, value -> {
                    assertThat(value, is(sample));
                });
    }

    @Test
    public void readUnversionedRow() {
        process(makeUnversionedRowCanonicalBlob(), reader -> {
            UnversionedRow sample = makeUnversionedRowSample();
            UnversionedRow row = reader.readUnversionedRow(new UnversionedRowDeserializer());
            assertThat(row, is(sample));
        });
    }

    @Test
    public void readUnversionedRowset() {
        process(makeUnversionedRowsetCanonicalBlob(), reader -> {
            UnversionedRow sample = makeUnversionedRowSample();
            final UnversionedRowsetDeserializer builder =
                    new UnversionedRowsetDeserializer(tableSchema(makeUnversionedRowCanonicalDescriptor()));
            final UnversionedRowset rowset = reader.readUnversionedRowset(builder).getRowset();
            assertEquals(1, rowset.getRows().size());

            UnversionedRow row = rowset.getRows().get(0);
            assertThat(row, is(sample));
        });

    }

    @Test
    public void readUnversionedRowMapped() {
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob()),
                makeUnversionedRowCanonicalDescriptor(), RowSampleOject.class, value -> {
                    final RowSampleOject sample = makeRowSampleObject();
                    assertThat(value, is(sample));
                });
    }

    @Test
    public void readUnversionedRowsetMapped() {
        process(Collections.singletonList(makeUnversionedRowsetCanonicalBlob()),
                makeUnversionedRowCanonicalDescriptor(), RowSampleOject.class, value -> {
                    final RowSampleOject sample = makeRowSampleObject();
                    assertThat(value, is(sample));
                }, WireProtocolReader::readUnversionedRowset);
    }

    @Test
    public void readUnversionRowMappedForAllFieldsDefault() {
        final RowSampleAllObject sample = new RowSampleAllObject();
        process(sample);
    }

    @Test
    public void readUnversionRowMappedForAllFieldsExceptClasses() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses();
        process(sample);
    }

    @Test
    public void readUnversionRowMappedForAllFieldsWithOneClass() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses();
        sample.setSampleObject(makeRowSampleObject());
        process(sample);
    }

    @Test(expected = RuntimeException.class)
    public void readUnversionRowMappedForAllFieldsWithPartialClass() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses();
        final RowSampleAllInternal1Object internal1Object = new RowSampleAllInternal1Object();
        sample.setInternalObject(internal1Object);

        // В текущей реализации десериализатора мы не можем не получить все внутренние поля объекта
        // Это нужно исправить (т.е. нужно пытаться маппить все, что можно)
        process(sample);
    }

    @Test
    public void readUnversionRowMappedForAllFieldsWithAllClasses() {
        final RowSampleAllObject sample = makeMappedObjectComplete();
        process(sample);
    }

    @Test
    public void readSchemafulRow() {
        process(makeSchemafulRowCanonicalBlob(), reader -> {
            final UnversionedRow sample = makeSchemafulRowSample();
            final SchemafulRowDeserializer builder =
                    new SchemafulRowDeserializer(extractSchemaData(sample, ColumnValueType.INT64));
            final UnversionedRow row = reader.readSchemafulRow(builder);
            assertThat(row, is(sample));
        });
    }

    @Test
    public void readSchemafulRowset() {
        process(makeSchemafulRowsetCanonicalBlob(), reader -> {
            final UnversionedRow sample = makeSchemafulRowSample();
            final SchemafulRowsetDeserializer builder =
                    new SchemafulRowsetDeserializer(tableSchema(makeSchemafulRowCanonicalDescriptor()));
            final UnversionedRowset rowset = reader.readSchemafulRowset(builder).getRowset();
            assertEquals(1, rowset.getRows().size());

            final UnversionedRow row = rowset.getRows().get(0);
            assertThat(row, is(sample));
        });
    }

    @Test
    public void readSchemafulRowMapped() {
        process(Collections.singletonList(makeSchemafulRowCanonicalBlob()),
                makeSchemafulRowCanonicalDescriptor(), RowSampleOject.class, value -> {
                    final RowSampleOject sample = makeRowSampleObject(false);
                    assertThat(value, is(sample));
                }, WireProtocolReader::readSchemafulRow);
    }

    @Test
    public void readSchemafulRowsetMapped() {
        process(Collections.singletonList(makeSchemafulRowsetCanonicalBlob()),
                makeSchemafulRowCanonicalDescriptor(), RowSampleOject.class, value -> {
                    final RowSampleOject sample = makeRowSampleObject(false);
                    assertThat(value, is(sample));
                }, WireProtocolReader::readSchemafulRowset);
    }

    @Ignore
    @Test
    public void writeVersionedRow() {
        final List<byte[]> data = new ArrayList<>();
        final WireProtocolWriter writer = new WireProtocolWriter(data);
        writer.writeVersionedRow(makeVersionedRowSample());
        writer.finish();

        Assert.assertEquals(1, data.size());
        System.out.print("0x");
        System.out.println(Hex.encodeHr(data.get(0)).replace(" ", ",0x"));
    }

    @Test
    public void readVersionedRow() {
        process(makeVersionedRowCanonicalBlob(), reader -> {
            final VersionedRow sample = makeVersionedRowSample();
            final VersionedRowDeserializer builder =
                    new VersionedRowDeserializer(WireProtocolReader.makeSchemaData(
                            tableSchema(makeVersionedRowCanonicalDescriptor())));
            final VersionedRow row = reader.readVersionedRow(builder);
            assertThat(row, is(sample));
        });
    }

    @Test
    public void readVersionedRowset() {
        process(makeVersionedRowsetCanonicalBlob(), reader -> {
            final VersionedRow sample = makeVersionedRowSample();
            final VersionedRowsetDeserializer builder =
                    new VersionedRowsetDeserializer(tableSchema(makeVersionedRowCanonicalDescriptor()));
            final VersionedRowset rowset = reader.readVersionedRowset(builder).getRowset();
            Assert.assertEquals(1, rowset.getRows().size());
            final VersionedRow row = rowset.getRows().get(0);
            assertThat(row, is(sample));
        });
    }

    @Test
    public void readVersionedRowMapped() {
        process(Collections.singletonList(makeVersionedRowCanonicalBlob()),
                makeVersionedRowCanonicalDescriptor(), RowSampleOject.class, value -> {
                    final RowSampleOject sample = makeRowSampleObject();
                    assertThat(value, is(sample));
                }, WireProtocolReader::readVersionedRow);
    }

    @Test
    public void readVersionedRowsetMapped() {
        process(Collections.singletonList(makeVersionedRowsetCanonicalBlob()),
                makeVersionedRowCanonicalDescriptor(), RowSampleOject.class, value -> {
                    final RowSampleOject sample = makeRowSampleObject();
                    assertThat(value, is(sample));
                }, WireProtocolReader::readVersionedRowset);
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
            final SchemafulRowDeserializer rowBuilder = new SchemafulRowDeserializer(blobSchema);
            UnversionedRow row = reader.readSchemafulRow(rowBuilder);
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
            UnversionedRow row = reader.readUnversionedRow(new UnversionedRowDeserializer());
            assertThat(row, is(expected));
        });
    }
}
