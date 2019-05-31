package ru.yandex.yt.ytclient.wire;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WireProtocolWriterTest extends WireProtocolTest {
    private static void output(TableSchema tableSchema, UnversionedRow row) {
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(row, new UnversionedRowSerializer(tableSchema));
        writer.finish();
    }

    private static void process(List<byte[]> expect, TableSchema tableSchema, UnversionedRow sample) {
        final UnversionedRowSerializer serializer = new UnversionedRowSerializer(tableSchema);

        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(sample, serializer);
        byte[] data = mergeChunks(writer.finish());
        assertThat(data, dataEquals(mergeChunks(expect)));
    }

    @SuppressWarnings("unchecked")
    private static <T> void process(List<byte[]> expect, T sample) {
        process(expect, (Class<T>) sample.getClass(), (writer, serializer) ->
                writer.writeUnversionedRow(sample, serializer));
    }

    private static <T> void process(List<byte[]> expect, Class<T> clazz,
            BiConsumer<WireProtocolWriter, WireRowSerializer<T>> writeFunction)
    {
        final YTreeObjectSerializer<T> serializer =
                (YTreeObjectSerializer<T>) YTreeObjectSerializerFactory.forClass(clazz);

        final MappedRowSerializer<T> rowSerializer = MappedRowSerializer.forClass(serializer);

        WireProtocolWriter writer = new WireProtocolWriter();
        writeFunction.accept(writer, rowSerializer);
        byte[] actual = mergeChunks(writer.finish());

        assertThat(actual, dataEquals(mergeChunks(expect)));
    }


    private static void process(RowSampleAllObject sample) {
        process(makeAttachmentsForMappedObject(sample, false), sample);
    }

    @Test
    public void writeUnversionedRow() {
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob()),
                tableSchema(makeUnversionedRowCanonicalDescriptor()), makeUnversionedRowSample());
    }

    @Test
    public void writeUnversionedRowNonAggr() {
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob(false)),
                tableSchema(makeUnversionedRowCanonicalDescriptor()), makeUnversionedRowSample(false));
    }

    @Test
    public void writeUnversionedRowMapped() {
        // Пока не поддерживаем простановку признака "aggregated"
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob(false)), makeRowSampleObject());
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsDefault() {
        // Текущий сериализатор не может замапить int, Integer, long и Long поля в UINT64
        final RowSampleAllObject sample = new RowSampleAllObject();
        process(sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsExceptClasses() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses();
        process(sample);
    }

    @Test
    public void writenversionRowMappedForAllFieldsWithOneClass() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses();
        sample.setSampleObject(makeRowSampleObject());
        process(sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsWithPartialClass() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses();
        final RowSampleAllInternal1Object internal1Object = new RowSampleAllInternal1Object();
        sample.setInternalObject(internal1Object);

        // В текущей реализации мы можем сериализовать класс частично, но получим ошибку при попытке десерализации такого класса
        process(sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsWithAllClasses() {
        final RowSampleAllObject sample = makeMappedObjectComplete();
        process(sample);
    }

    @Test
    public void writeSchemafulRow() {
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeSchemafulRow(makeSchemafulRowSample());
        byte[] data = mergeChunks(writer.finish());
        assertThat(data, dataEquals(makeSchemafulRowCanonicalBlob()));
    }

    @Test
    public void writeVeryBigRow() {
        // Тест на регрессию баги с перепутанными Math.min и Math.max
        byte[] data = new byte[32768]; // строчка нулей в 32КБ
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(new UnversionedRow(Collections.singletonList(
                new UnversionedValue(0, ColumnValueType.STRING, false, data)
        )), new UnversionedRowSerializer(tableSchema(makeUnversionedRowCanonicalDescriptor())));
        List<byte[]> chunks = writer.finish();
        // Проверяем, что на выходе получился 1 чанк
        assertThat(chunks.size(), is(1));
        // Проверяем, что на выходе 16 (кол-во колонок и заголовок колонки) + 32768 байт
        assertThat(chunks.get(0).length, is(16 + data.length));
    }

    @Test
    public void writeVeryBigRowOnAllocationBound() {
        // Проверяем, что мы не потеряли вычисленное количество полей при сериализации объекта с маппером

        // Размер префетча: 4096 байт
        final byte[] data0 = new byte[4096];

        // Второй префетч: 8192 байт
        final byte[] data1 = new byte[8200];

        final int maxChunkSize = 8200;

        final byte[] unversionedChunks;
        final byte[] mappedChunks;
        {
            final UnversionedRowSerializer serializer =
                    new UnversionedRowSerializer(tableSchema(makeUnversionedRowCanonicalDescriptor()));

            final WireProtocolWriter writer = new WireProtocolWriter(new ArrayList<>(), maxChunkSize);
            writer.writeUnversionedRow(new UnversionedRow(Collections.singletonList(
                    new UnversionedValue(0, ColumnValueType.STRING, false, data0)
            )), serializer);
            writer.finish();

            writer.writeUnversionedRow(new UnversionedRow(Collections.singletonList(
                    new UnversionedValue(0, ColumnValueType.STRING, false, data1)
            )), serializer);

            final List<byte[]> data = writer.finish();
            assertThat(data.size(), is(3));

            unversionedChunks = mergeChunks(data);
            assertThat(unversionedChunks.length, is(16 + data0.length + 16 + data1.length));
        }
        {
            final MappedRowSerializer<SingleColumnClass> serializer =
                    MappedRowSerializer.forClass((YTreeObjectSerializer<SingleColumnClass>) YTreeObjectSerializerFactory
                            .forClass(SingleColumnClass.class));

            final WireProtocolWriter writer = new WireProtocolWriter(new ArrayList<>(), maxChunkSize);
            writer.writeUnversionedRow(new SingleColumnClass()
                    .setLargeColumn(new String(data0, StandardCharsets.UTF_8)), serializer);
            writer.finish();

            writer.writeUnversionedRow(new SingleColumnClass()
                    .setLargeColumn(new String(data1, StandardCharsets.UTF_8)), serializer);

            final List<byte[]> data = writer.finish();
            assertThat(data.size(), is(3));

            mappedChunks = mergeChunks(data);
            assertThat(mappedChunks.length, is(16 + data0.length + 16 + data1.length));
        }
        Assert.assertArrayEquals(unversionedChunks, mappedChunks);
        assertThat(mappedChunks, dataEquals(unversionedChunks));
    }

    @Test
    public void writeManySmallRows() {
        // Тест на регрессию баги с неправильным использованием ByteBuffer.wrap
        byte[] data = new byte[1024]; // строчка в 1KB
        WireProtocolWriter writer = new WireProtocolWriter();
        for (int i = 0; i < 32; ++i) {
            writer.writeUnversionedRow(new UnversionedRow(Collections.singletonList(
                    new UnversionedValue(0, ColumnValueType.STRING, false, data)
            )), new UnversionedRowSerializer(tableSchema(makeUnversionedRowCanonicalDescriptor())));
        }
        List<byte[]> chunks = writer.finish();
        // Проверяем, что на выходе получился 1 чанк
        assertThat(chunks.size(), is(1));
        // Проверяем, что на выходе каждой строчки 16 (кол-во колонок и заголовок колонки) + 1024 байта
        assertThat(chunks.get(0).length, is(32 * (16 + 1024)));
    }

    @YTreeObject
    public static class SingleColumnClass {
        @YTreeKeyField
        public String largeColumn;

        public SingleColumnClass setLargeColumn(String largeColumn) {
            this.largeColumn = largeColumn;
            return this;
        }
    }
}
