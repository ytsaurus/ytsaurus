package ru.yandex.yt.ytclient.wire;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.misc.lang.number.UnsignedLong;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WireProtocolWriterTest extends WireProtocolTest {

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

    private static <T> void process(List<byte[]> expect, YTreeObjectSerializer<T> serializer, T sample) {
        process(expect, serializer, (BiConsumer<WireProtocolWriter, WireRowSerializer<T>>) (writer, s) ->
                writer.writeUnversionedRow(sample, s));
    }

    private static <T> void process(List<byte[]> expect, Class<T> clazz,
                                    BiConsumer<WireProtocolWriter, WireRowSerializer<T>> writeFunction) {
        final YTreeObjectSerializer<T> serializer =
                (YTreeObjectSerializer<T>) YTreeObjectSerializerFactory.forClass(clazz);
        process(expect, serializer, writeFunction);
    }

    private static <T> void process(List<byte[]> expect, YTreeObjectSerializer<T> serializer,
                                    BiConsumer<WireProtocolWriter, WireRowSerializer<T>> writeFunction) {
        final MappedRowSerializer<T> rowSerializer = MappedRowSerializer.forClass(serializer);

        WireProtocolWriter writer = new WireProtocolWriter();
        writeFunction.accept(writer, rowSerializer);
        byte[] actual = mergeChunks(writer.finish());

        assertThat(actual, dataEquals(mergeChunks(expect)));
    }


    private static void process(RowSampleAllObject sample) {
        process(makeAttachmentsForMappedObject_For_RowSampleAllObject(sample, false), sample);
    }

    @Test
    public void writeUnversionedRow() {
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob_For_RowSample()),
                tableSchema(makeDescriptor_For_RowSample()), makeUnversionedRow_For_RowSample());
    }

    @Test
    public void writeUnversionedRowNonAggr() {
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob_For_RowSample(false)),
                tableSchema(makeDescriptor_For_RowSample()), makeUnversionedRow_For_RowSample(false));
    }

    @Test
    public void writeUnversionedRowMapped() {
        // Пока не поддерживаем простановку признака "aggregated"
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob_For_RowSample(false)),
                makeSample_For_RowSampleObject());
    }

    @Test
    public void writeUnversionedRowMappedWithoutFewFields() {
        // Исключаем поля vInt64 из списка вставляемых vString
        var exclude = Set.of("vInt64", "vString");
        var serializer = new YTreeObjectSerializer<>(RowSampleObject.class,
                field -> !exclude.contains(field.getName()));
        process(Collections.singletonList(makeUnversionedRowCanonicalBlob_For_RowSampleNoVInt64NoVString()),
                serializer, makeSample_For_RowSampleObject());
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsDefault() {
        // Текущий сериализатор не может замапить int, Integer, long и Long поля в UINT64
        final RowSampleAllObject sample = new RowSampleAllObject();
        process(sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsExceptClasses() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses_For_RowSampleAllObject();
        process(sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsWithOneClass() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses_For_RowSampleAllObject();
        sample.setSampleObject(makeSample_For_RowSampleObject());
        process(sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsWithPartialClass() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses_For_RowSampleAllObject();
        final RowSampleAllInternal1Object internal1Object = new RowSampleAllInternal1Object();
        sample.setInternalObject(internal1Object);

        // В текущей реализации мы можем сериализовать класс частично, но получим ошибку при попытке десерализации
        // такого класса
        process(sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsWithAllClasses() {
        final RowSampleAllObject sample = makeMappedObjectComplete_For_RowSampleAllObject();
        process(sample);
    }


    @Test
    public void writeUnversionRowMappedForAllFieldsWithAllClassesAndStateSupportCheckNotTheSame() {
        final RowSampleAllObject sample = makeMappedObjectComplete_For_RowSampleAllObject();
        sample.saveYTreeObjectState();

        final RowSampleAllObject copy = sample.getYTreeObjectState();
        Assert.assertEquals(copy, sample);

        Assert.assertNotSame(copy, sample);

        Assert.assertSame(copy.getInt64_as_Integer(), sample.getInt64_as_Integer());
        Assert.assertSame(copy.getInt64_as_Long(), sample.getInt64_as_Long());
        Assert.assertSame(copy.getUint64_as_Integer(), sample.getUint64_as_Integer());
        Assert.assertSame(copy.getUint64_as_Long(), sample.getUint64_as_Long());
        Assert.assertSame(copy.getUint64_as_UnsignedLong(), sample.getUint64_as_UnsignedLong());
        Assert.assertSame(copy.getDouble_as_Double(), sample.getDouble_as_Double());
        Assert.assertSame(copy.getBoolean_as_Boolean(), sample.getBoolean_as_Boolean());
        Assert.assertSame(copy.getString_as_string(), sample.getString_as_string());
        Assert.assertNotSame(copy.getString_as_bytes(), sample.getString_as_bytes());
        Assert.assertNull(copy.getAny_as_string());
        Assert.assertNull(sample.getAny_as_string());
        Assert.assertNull(copy.getAny_as_bytes());
        Assert.assertNull(sample.getAny_as_bytes());
        Assert.assertNotSame(copy.getSampleObject(), sample.getSampleObject());
        Assert.assertNotSame(copy.getInternalObject(), sample.getInternalObject());
        Assert.assertNotSame(copy.getFlatten(), sample.getFlatten());
        Assert.assertNotSame(copy.getSimpleMapObject(), sample.getSimpleMapObject());
        Assert.assertNotSame(copy.getComplexMapObject(), sample.getComplexMapObject());
        Assert.assertNotSame(copy.getSimpleListObject(), sample.getSimpleListObject());
        Assert.assertNotSame(copy.getComplexListObject(), sample.getComplexListObject());
        Assert.assertNotSame(copy.getSimpleArrayObject(), sample.getSimpleArrayObject());
        Assert.assertNotSame(copy.getComplexArrayObject(), sample.getComplexArrayObject());
        Assert.assertNotSame(copy.getPrimitiveArrayObject(), sample.getPrimitiveArrayObject());
        Assert.assertNotSame(copy.getSimpleSetObjects(), sample.getSimpleSetObjects());
        Assert.assertNotSame(copy.getComplexSetObjects(), sample.getComplexSetObjects());

    }

    @Test
    public void writeUnversionRowMappedForAllFieldsWithAllClassesAndStateSupportUnchanged() {
        final RowSampleAllObject sample = makeMappedObjectComplete_For_RowSampleAllObject();
        sample.saveYTreeObjectState();

        // Ни одно поле не изменилось - будет сохранено только значение  int64_as_int (ключ)

        final UnversionedRow row = new UnversionedRow(Collections.singletonList(
                new UnversionedValue(0, ColumnValueType.INT64, false, (long) sample.getInt64_as_int())));

        final WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(row,
                new UnversionedRowSerializer(tableSchema(makeDescriptor_For_RowSampleAllObject())));
        final List<byte[]> expect = writer.finish();

        process(expect, sample);
    }

    @Test
    public void writeUnversionRowMappedForAllFieldsWithAllClassesAndStateSupportChanged() {
        final RowSampleAllObject sample = makeMappedObjectComplete_For_RowSampleAllObject();
        sample.saveYTreeObjectState();
        // Поправим все поля и убедимся, что они будут корректно сериализованы (т.е. изменения всех полей были
        // задетекчены)

        sample.setInt64_as_int(sample.getInt64_as_int() + 1);
        sample.setInt64_as_Integer(sample.getInt64_as_Integer() + 1);

        sample.setInt64_as_long(sample.getInt64_as_long() + 1);
        sample.setInt64_as_Long(sample.getInt64_as_Long() + 1);

        sample.setUint64_as_int(sample.getUint64_as_int() + 1);
        sample.setUint64_as_Integer(sample.getUint64_as_Integer() + 1);

        sample.setUint64_as_long(sample.getUint64_as_long() + 1);
        sample.setUint64_as_Long(sample.getUint64_as_Long() + 1);

        sample.setUint64_as_UnsignedLong(UnsignedLong.valueOf(sample.getUint64_as_UnsignedLong().longValue() + 1));

        sample.setDouble_as_double(sample.getDouble_as_double() + 1);
        sample.setDouble_as_Double(sample.getDouble_as_Double() + 1);

        sample.setBoolean_as_boolean(!sample.isBoolean_as_boolean());
        sample.setBoolean_as_Boolean(!sample.getBoolean_as_Boolean());

        sample.setString_as_string(sample.getString_as_string() + "+1");
        sample.getString_as_bytes()[0] = (byte) (sample.getString_as_bytes()[0] + 1);

        sample.setAny_as_string(sample.getAny_as_string() + "+1");
        sample.setAny_as_bytes(ArrayUtils.add(sample.getAny_as_bytes(), (byte) 1));

        sample.getSampleObject().setvBoolean(!sample.getSampleObject().isvBoolean());
        sample.getInternalObject().getRowInternalObject().getFlattenObject().setF2("new-f2");
        sample.getFlatten().setF1("new-f1");

        sample.getSimpleMapObject().put("new-key", "new-value");
        sample.getComplexMapObject().values().iterator().next().setvString("new-complex-1");

        sample.getSimpleListObject().add("new-value");
        sample.getComplexListObject().iterator().next().setvString("new-complex-2");

        sample.setSimpleArrayObject(ArrayUtils.add(sample.getSimpleArrayObject(), "new-array-1"));
        sample.getComplexArrayObject()[0].setvString("fixed-array-2");
        sample.getPrimitiveArrayObject()[0] = sample.getPrimitiveArrayObject()[0] + 1;

        sample.getSimpleSetObjects().add("set-1");
        sample.getComplexSetObjects().iterator().next().setvString("set-2");

        process(sample);
    }

    @Test
    public void writeSchemafulRow() {
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeSchemafulRow(makeSchemaful_For_RowSample());
        byte[] data = mergeChunks(writer.finish());
        assertThat(data, dataEquals(makeSchemafulRowCanonicalBlob_For_RowSample()));
    }

    @Test
    public void writeVeryBigRow() {
        // Тест на регрессию баги с перепутанными Math.min и Math.max
        byte[] data = new byte[32768]; // строчка нулей в 32КБ
        WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(new UnversionedRow(Collections.singletonList(
                new UnversionedValue(0, ColumnValueType.STRING, false, data)
        )), new UnversionedRowSerializer(tableSchema(makeDescriptor_For_RowSample())));
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
                    new UnversionedRowSerializer(tableSchema(makeDescriptor_For_RowSample()));

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
            )), new UnversionedRowSerializer(tableSchema(makeDescriptor_For_RowSample())));
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
