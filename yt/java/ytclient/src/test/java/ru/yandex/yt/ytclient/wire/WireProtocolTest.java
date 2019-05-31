package ru.yandex.yt.ytclient.wire;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.ArrayUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeStringNodeImpl;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.misc.codec.Hex;
import ru.yandex.misc.lang.number.UnsignedLong;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.ApiServiceUtil;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

/**
 * Общие утилиты для тестов wire протокола
 * <p>
 * Основано на данных из yt/unittests/wire_protocol_ut.cpp
 */
public class WireProtocolTest {
    private static final ColumnValueType[] VALUE_TYPES = new ColumnValueType[]{
            ColumnValueType.NULL,
            ColumnValueType.INT64,
            ColumnValueType.UINT64,
            ColumnValueType.DOUBLE,
            ColumnValueType.BOOLEAN,
            ColumnValueType.STRING,
            ColumnValueType.ANY,
    };

    private static final boolean[] AGGREGATE_TYPES = new boolean[]{true, false};

    public static byte[] makeByteArray(int... bytes) {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            result[i] = (byte) bytes[i];
        }
        return result;
    }

    public static byte[] mergeChunks(List<byte[]> chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }
        byte[] result = new byte[length];
        int index = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, result, index, chunk.length);
            index += chunk.length;
        }
        return result;
    }

    public static Object makeSampleValue(ColumnValueType type) {
        switch (type) {
            case INT64:
                return Long.parseLong("0123456789ABCDEF", 16);
            case UINT64:
                return Long.parseUnsignedLong("FEDCBA9876543210", 16);
            case DOUBLE:
                return 3.141592653589793;
            case BOOLEAN:
                return false;
            case STRING:
                return new byte[]{'s'};
            case ANY:
                return new byte[]{'{', '}'};
            default:
                return null;
        }
    }

    public static UnversionedValue makeValueSample(int id, ColumnValueType type, boolean aggregate) {
        Object value = makeSampleValue(type);
        if (type == ColumnValueType.ANY && value instanceof byte[]) {
            value = UnversionedValue.convertValueTo(new YTreeStringNodeImpl((byte[]) value, Cf.map()), type);
        }
        return new UnversionedValue(id, type, aggregate, value);
    }

    public static UnversionedRow makeUnversionedRowSample() {
        return makeUnversionedRowSample(true);
    }

    public static UnversionedRow makeUnversionedRowSample(boolean fillAggr) {
        List<UnversionedValue> values = new ArrayList<>();
        int id = 0;
        for (ColumnValueType type : VALUE_TYPES) {
            for (boolean aggregate : AGGREGATE_TYPES) {
                values.add(makeValueSample(id++, type, aggregate && fillAggr));
            }
        }
        return new UnversionedRow(values);
    }

    public static TableSchema tableSchema(TRowsetDescriptor descriptor) {
        return ApiServiceUtil.deserializeRowsetSchema(descriptor);
    }

    public static TRowsetDescriptor makeDescriptorForMappedObject() {
        final TRowsetDescriptor.Builder rowset = TRowsetDescriptor.newBuilder();
        rowset.addColumnsBuilder().setName("int64_as_int").setType(ColumnValueType.INT64.getValue());
        rowset.addColumnsBuilder().setName("int64_as_Integer").setType(ColumnValueType.INT64.getValue());
        rowset.addColumnsBuilder().setName("int64_as_long").setType(ColumnValueType.INT64.getValue());
        rowset.addColumnsBuilder().setName("int64_as_Long").setType(ColumnValueType.INT64.getValue());

        rowset.addColumnsBuilder().setName("uint64_as_int").setType(ColumnValueType.UINT64.getValue());
        rowset.addColumnsBuilder().setName("uint64_as_Integer").setType(ColumnValueType.UINT64.getValue());
        rowset.addColumnsBuilder().setName("uint64_as_long").setType(ColumnValueType.UINT64.getValue());
        rowset.addColumnsBuilder().setName("uint64_as_Long").setType(ColumnValueType.UINT64.getValue());
        rowset.addColumnsBuilder().setName("uint64_as_UnsignedLong").setType(ColumnValueType.UINT64.getValue());

        rowset.addColumnsBuilder().setName("double_as_double").setType(ColumnValueType.DOUBLE.getValue());
        rowset.addColumnsBuilder().setName("double_as_Double").setType(ColumnValueType.DOUBLE.getValue());

        rowset.addColumnsBuilder().setName("boolean_as_boolean").setType(ColumnValueType.BOOLEAN.getValue());
        rowset.addColumnsBuilder().setName("boolean_as_Boolean").setType(ColumnValueType.BOOLEAN.getValue());

        rowset.addColumnsBuilder().setName("string_as_string").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("string_as_bytes").setType(ColumnValueType.STRING.getValue());

        rowset.addColumnsBuilder().setName("any_as_string").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("any_as_bytes").setType(ColumnValueType.ANY.getValue());

        rowset.addColumnsBuilder().setName("sampleObject").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("internalObject").setType(ColumnValueType.ANY.getValue());

        rowset.addColumnsBuilder().setName("f1").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("f21").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("f22").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("internalObject3").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("f31").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("f32").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("f2").setType(ColumnValueType.STRING.getValue());

        rowset.addColumnsBuilder().setName("simpleMapObject").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("complexMapObject").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("simpleListObject").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("complexListObject").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("simpleArrayObject").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("complexArrayObject").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("primitiveArrayObject").setType(ColumnValueType.ANY.getValue());

        return rowset.build();
    }

    public static RowSampleAllObject makeMappedObjectWithoutClasses() {
        final RowSampleAllObject sample = new RowSampleAllObject();
        sample.setInt64_as_int(3);
        sample.setInt64_as_Integer(4);
        sample.setInt64_as_long(6);
        sample.setInt64_as_Long(5L);


        sample.setUint64_as_int(7);
        sample.setUint64_as_Integer(8);
        sample.setUint64_as_long(9);
        sample.setUint64_as_Long(10L);

        sample.setUint64_as_UnsignedLong(UnsignedLong.valueOf(12));

        sample.setDouble_as_Double(1.0);
        sample.setDouble_as_double(2.0);

        sample.setBoolean_as_Boolean(true);
        sample.setBoolean_as_boolean(true);

        sample.setString_as_string("test string");
        sample.setString_as_bytes("test 2 string".getBytes());
        return sample;
    }

    public static RowSampleAllObject makeMappedObjectComplete() {
        final RowSampleAllObject sample = makeMappedObjectWithoutClasses();
        sample.setSampleObject(makeRowSampleObject());

        final RowSampleAllInternal2Object internal2Object = new RowSampleAllInternal2Object();
        internal2Object.setKey(Integer.MAX_VALUE - 1);
        internal2Object.setRowSampleOject(makeRowSampleObject());

        final RowSampleAllFlattenObject flattenObject = new RowSampleAllFlattenObject();
        flattenObject.setF1("is1");
        flattenObject.setF2("is2");
        final RowSampleAllFlatten2Object flatten2Object = new RowSampleAllFlatten2Object();
        flatten2Object.setF21("is21");
        flatten2Object.setF22("is22");
        final RowSampleAllInternal3Object internal3Object = new RowSampleAllInternal3Object();
        internal3Object.setKey(100500);
        flatten2Object.setInternalObject3(internal3Object);
        flattenObject.setFlatten2(flatten2Object);
        final RowSampleAllFlatten3Object flatten3Object = new RowSampleAllFlatten3Object();
        flatten3Object.setF31("is31");
        flatten3Object.setF32("is32");
        flattenObject.setFlatten3(flatten3Object);
        internal2Object.setFlattenObject(flattenObject);

        final RowSampleAllInternal1Object internal1Object = new RowSampleAllInternal1Object();
        internal1Object.setKey(Integer.MAX_VALUE - 2);
        internal1Object.setRowSampleOject(makeRowSampleObject());
        internal1Object.setRowInternalObject(internal2Object);

        sample.setInternalObject(internal1Object);

        sample.getSimpleMapObject0().put("s1", "v1");
        sample.getSimpleMapObject0().put("s2", "v2");

        sample.getComplexMapObject0().put("k1", makeRowSampleObject());
        sample.getComplexMapObject0().put("k2", makeRowSampleObject());

        sample.getSimpleListObject0().add("s1");
        sample.getSimpleListObject0().add("s2");

        sample.getComplexListObject0().add(makeRowSampleObject());
        sample.getComplexListObject0().add(makeRowSampleObject());

        sample.setSimpleArrayObject("a1", "a2");
        sample.setComplexArrayObject(makeRowSampleObject(), makeRowSampleObject());

        sample.setPrimitiveArrayObject(1, 2, 3, 4, 5);

        final RowSampleAllFlattenObject flattenTop = new RowSampleAllFlattenObject();
        flattenTop.setF1("fs1");
        flattenTop.setF2("fs2");
        final RowSampleAllFlatten2Object flatten2Top = new RowSampleAllFlatten2Object();
        flatten2Top.setF21("fs21");
        flatten2Top.setF22("fs22");
        flattenTop.setFlatten2(flatten2Top);
        final RowSampleAllFlatten3Object flatten3Top = new RowSampleAllFlatten3Object();
        flatten3Top.setF31("fs31");
        flatten3Top.setF32("fs32");
        flattenTop.setFlatten3(flatten3Top);

        final RowSampleAllInternal3Object sample3 = new RowSampleAllInternal3Object();
        sample3.setKey(999);
        flatten2Object.setInternalObject3(sample3);
        sample.setFlatten(flattenTop);

        return sample;
    }

    public static UnversionedRow makeUnversionedRowForMappedObject(RowSampleAllObject object,
                                                                   boolean ignoreCompatibility) {
        final List<UnversionedValue> values = new ArrayList<>();
        final AtomicInteger inc = new AtomicInteger();

        final TriConsumer<ColumnValueType, Object, YTreeSerializer<?>> add0 = (type, value, serializer) -> {
            if (value != null) {
                values.add(new UnversionedValue(inc.get(), type, false, mapValue(value, type, serializer)));
            }
            inc.incrementAndGet(); // Всегда инкрементируем счетчик
        };

        final BiConsumer<ColumnValueType, Object> add = (type, value) -> add0.accept(type, value, null);


        add.accept(ColumnValueType.INT64, object.getInt64_as_int());
        add.accept(ColumnValueType.INT64, object.getInt64_as_Integer());
        add.accept(ColumnValueType.INT64, object.getInt64_as_long());
        add.accept(ColumnValueType.INT64, object.getInt64_as_Long());

        final ColumnValueType uintType = ignoreCompatibility ? ColumnValueType.UINT64 : ColumnValueType.INT64;
        add.accept(uintType, object.getUint64_as_int());
        add.accept(uintType, object.getUint64_as_Integer());
        add.accept(uintType, object.getUint64_as_long());
        add.accept(uintType, object.getUint64_as_Long());
        add.accept(ColumnValueType.UINT64, object.getUint64_as_UnsignedLong());

        add.accept(ColumnValueType.DOUBLE, object.getDouble_as_double());
        add.accept(ColumnValueType.DOUBLE, object.getDouble_as_Double());

        add.accept(ColumnValueType.BOOLEAN, object.isBoolean_as_boolean());
        add.accept(ColumnValueType.BOOLEAN, object.getBoolean_as_Boolean());

        add.accept(ColumnValueType.STRING, object.getString_as_string());
        add.accept(ignoreCompatibility ? ColumnValueType.STRING : ColumnValueType.ANY, object.getString_as_bytes());

        add.accept(ignoreCompatibility ? ColumnValueType.ANY : ColumnValueType.STRING, object.getAny_as_string());
        add.accept(ColumnValueType.ANY, object.getAny_as_bytes());

        add.accept(ColumnValueType.ANY, object.getSampleObject());
        add.accept(ColumnValueType.ANY, object.getInternalObject());


        final RowSampleAllFlattenObject flatten = object.getFlatten();
        if (flatten != null) {
            add.accept(ColumnValueType.STRING, flatten.getF1());
            final RowSampleAllFlatten2Object flatte2 = flatten.getFlatten2();
            if (flatte2 != null) {
                add.accept(ColumnValueType.STRING, flatte2.getF21());
                add.accept(ColumnValueType.STRING, flatte2.getF22());
                add.accept(ColumnValueType.ANY, flatte2.getInternalObject3());
            }
            final RowSampleAllFlatten3Object flatten3 = flatten.getFlatten3();
            if (flatten3 != null) {
                add.accept(ColumnValueType.STRING, flatten3.getF31());
                add.accept(ColumnValueType.STRING, flatten3.getF32());
            }
            add.accept(ColumnValueType.STRING, flatten.getF2());
        }

        final YTreeObjectSerializer<RowSampleAllObject> serializer =
                (YTreeObjectSerializer<RowSampleAllObject>) YTreeObjectSerializerFactory
                        .forClass(RowSampleAllObject.class);
        try {
            add0.accept(ColumnValueType.ANY, object.getSimpleMapObject(),
                    serializer.getField("simpleMapObject").getOrThrow("simpleMapObject").serializer);
            add0.accept(ColumnValueType.ANY, object.getComplexMapObject(),
                    serializer.getField("complexMapObject").getOrThrow("complexMapObject").serializer);
            add0.accept(ColumnValueType.ANY, object.getSimpleListObject(),
                    serializer.getField("simpleListObject").getOrThrow("simpleListObject").serializer);
            add0.accept(ColumnValueType.ANY, object.getComplexListObject(),
                    serializer.getField("complexListObject").getOrThrow("complexListObject").serializer);
            add.accept(ColumnValueType.ANY, object.getSimpleArrayObject());
            add0.accept(ColumnValueType.ANY, object.getComplexArrayObject(),
                    serializer.getField("complexArrayObject").getOrThrow("complexArrayObject").serializer);
            add.accept(ColumnValueType.ANY, object.getPrimitiveArrayObject());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new UnversionedRow(values);
    }

    public static List<byte[]> makeAttachmentsForMappedObject(RowSampleAllObject object) {
        return makeAttachmentsForMappedObject(object, true);
    }

    public static List<byte[]> makeAttachmentsForMappedObject(RowSampleAllObject object, boolean ignoreCompatibility) {
        final WireProtocolWriter writer = new WireProtocolWriter();
        writer.writeUnversionedRow(makeUnversionedRowForMappedObject(object, ignoreCompatibility),
                new UnversionedRowSerializer(tableSchema(makeDescriptorForMappedObject())));
        return writer.finish();
    }

    private static Object mapValue(Object value, ColumnValueType type, YTreeSerializer serializer) {
        Objects.requireNonNull(value);
        if (type == ColumnValueType.UINT64 && value instanceof UnsignedLong) {
            return ((UnsignedLong) value).longValue();
        } else if (type == ColumnValueType.ANY && (value.getClass().getDeclaredAnnotation(YTreeObject.class) != null
                || serializer != null)) {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            //noinspection unchecked
            YTreeBinarySerializer.serializeObj(value, serializer != null ? serializer :
                    (YTreeSerializer<Object>) YTreeObjectSerializerFactory.forClass(value.getClass()), outputStream);
            return outputStream.toByteArray();
        } else if (type == ColumnValueType.ANY && value instanceof byte[]) {
            // Набор байтов будет обернут в YSON объект
            return UnversionedValue.convertValueTo(new YTreeStringNodeImpl((byte[]) value, Cf.map()), type);
        } else {
            return UnversionedValue.convertValueTo(value, type);
        }
    }

    public static RowSampleOject makeRowSampleObject() {
        return makeRowSampleObject(true);
    }

    @SuppressWarnings("ConstantConditions")
    public static RowSampleOject makeRowSampleObject(boolean fillAggr) {
        final RowSampleOject sample = new RowSampleOject();
        sample.setvInt64((Long) makeSampleValue(ColumnValueType.INT64));
        sample.setvUint64(UnsignedLong.valueOf((Long) makeSampleValue(ColumnValueType.UINT64)));
        sample.setvDouble((Double) makeSampleValue(ColumnValueType.DOUBLE));
        sample.setvBoolean((Boolean) makeSampleValue(ColumnValueType.BOOLEAN));
        sample.setvString(new String((byte[]) makeSampleValue(ColumnValueType.STRING), StandardCharsets.UTF_8));
        sample.setvAny((byte[]) makeSampleValue(ColumnValueType.ANY));
        if (fillAggr) {
            sample.setvInt64Aggr((Long) makeSampleValue(ColumnValueType.INT64));
            sample.setvUint64Aggr(UnsignedLong.valueOf((Long) makeSampleValue(ColumnValueType.UINT64)));
            sample.setvDoubleAggr((Double) makeSampleValue(ColumnValueType.DOUBLE));
            sample.setvBooleanAggr((Boolean) makeSampleValue(ColumnValueType.BOOLEAN));
            sample.setvStringAggr(new String((byte[]) makeSampleValue(ColumnValueType.STRING), StandardCharsets.UTF_8));
            sample.setvAnyAggr((byte[]) makeSampleValue(ColumnValueType.ANY));
        }
        return sample;
    }

    public static TRowsetDescriptor makeUnversionedRowCanonicalDescriptor() {
        final TRowsetDescriptor.Builder rowset = TRowsetDescriptor.newBuilder();
        rowset.addColumnsBuilder().setName("vNull").setType(ColumnValueType.NULL.getValue());
        rowset.addColumnsBuilder().setName("vNullAggr").setType(ColumnValueType.NULL.getValue());
        rowset.addColumnsBuilder().setName("vInt64").setType(ColumnValueType.INT64.getValue());
        rowset.addColumnsBuilder().setName("vInt64Aggr").setType(ColumnValueType.INT64.getValue());
        rowset.addColumnsBuilder().setName("vUint64").setType(ColumnValueType.UINT64.getValue());
        rowset.addColumnsBuilder().setName("vUint64Aggr").setType(ColumnValueType.UINT64.getValue());
        rowset.addColumnsBuilder().setName("vDouble").setType(ColumnValueType.DOUBLE.getValue());
        rowset.addColumnsBuilder().setName("vDoubleAggr").setType(ColumnValueType.DOUBLE.getValue());
        rowset.addColumnsBuilder().setName("vBoolean").setType(ColumnValueType.BOOLEAN.getValue());
        rowset.addColumnsBuilder().setName("vBooleanAggr").setType(ColumnValueType.BOOLEAN.getValue());
        rowset.addColumnsBuilder().setName("vString").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("vStringAggr").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("vAny").setType(ColumnValueType.ANY.getValue());
        rowset.addColumnsBuilder().setName("vAnyAggr").setType(ColumnValueType.ANY.getValue());
        return rowset.build();
    }

    public static byte[] makeUnversionedRowsetCanonicalBlob() {
        return ArrayUtils.addAll(makeByteArray(
                // row count
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        ), makeUnversionedRowCanonicalBlob());
    }

    public static byte[] makeUnversionedRowCanonicalBlob() {
        return makeUnversionedRowCanonicalBlob(true);
    }

    public static byte[] makeUnversionedRowCanonicalBlob(boolean fillAggr) {
        final int AGGR = fillAggr ? 0x01 : 0x00;
        return makeByteArray(
                // value count
                0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // one value per row
                0x00, 0x00, 0x02, AGGR, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x03, AGGR, 0x00, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x03, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x04, 0x00, 0x04, AGGR, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x05, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x06, 0x00, 0x05, AGGR, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
                0x07, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
                0x08, 0x00, 0x06, AGGR, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x09, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x0A, 0x00, 0x10, AGGR, 0x01, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
                0x0B, 0x00, 0x10, 0x00, 0x01, 0x00, 0x00, 0x00, 0x73, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf, 0xcf,
                0x0C, 0x00, 0x11, AGGR, 0x04, 0x00, 0x00, 0x00, 0x01, 0x04, 0x7b, 0x7d, 0x00, 0x00, 0x00, 0x00,
                0x0D, 0x00, 0x11, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x04, 0x7b, 0x7d, 0x00, 0x00, 0x00, 0x00);
    }

    public static TRowsetDescriptor makeSchemafulRowCanonicalDescriptor() {
        final TRowsetDescriptor.Builder rowset = TRowsetDescriptor.newBuilder();
        rowset.addColumnsBuilder().setName("vNull").setType(ColumnValueType.NULL.getValue());
        rowset.addColumnsBuilder().setName("vInt64").setType(ColumnValueType.INT64.getValue());
        rowset.addColumnsBuilder().setName("vUint64").setType(ColumnValueType.UINT64.getValue());
        rowset.addColumnsBuilder().setName("vDouble").setType(ColumnValueType.DOUBLE.getValue());
        rowset.addColumnsBuilder().setName("vBoolean").setType(ColumnValueType.BOOLEAN.getValue());
        rowset.addColumnsBuilder().setName("vString").setType(ColumnValueType.STRING.getValue());
        rowset.addColumnsBuilder().setName("vAny").setType(ColumnValueType.ANY.getValue());
        return rowset.build();
    }


    public static UnversionedRow makeSchemafulRowSample() {
        List<UnversionedValue> values = new ArrayList<>();
        int id = 0;
        for (ColumnValueType type : VALUE_TYPES) {
            // no aggregates
            values.add(makeValueSample(id++, type, false));
        }
        return new UnversionedRow(values);
    }

    public static byte[] makeSchemafulRowsetCanonicalBlob() {
        return ArrayUtils.addAll(makeByteArray(
                // row count
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        ), makeSchemafulRowCanonicalBlob());
    }

    public static byte[] makeSchemafulRowCanonicalBlob() {

        return makeByteArray(
                // value count
                0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // null bitmap
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // one value per row
                0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x04, 0x7b, 0x7d, 0x00, 0x00, 0x00, 0x00);
    }

    public static VersionedRow makeVersionedRowSample() {
        final List<UnversionedValue> keys = new ArrayList<>();
        final List<VersionedValue> values = new ArrayList<>();
        int id = 0;
        int ts = 1000;
        for (ColumnValueType type : VALUE_TYPES) {
            for (boolean aggregate : AGGREGATE_TYPES) {
                final boolean isKey = type == ColumnValueType.NULL;
                final UnversionedValue value = makeValueSample(id++, type, aggregate && !isKey);
                if (isKey) {
                    keys.add(value);
                } else {
                    values.add(new VersionedValue(value.getId(), value.getType(), value.isAggregate(), value.getValue(),
                            ts++));
                }
            }
        }
        return new VersionedRow(Arrays.asList(1L, 2L), Arrays.asList(3L, 4L), keys, values);
    }

    public static TRowsetDescriptor makeVersionedRowCanonicalDescriptor() {
        return makeUnversionedRowCanonicalDescriptor();
    }

    public static byte[] makeVersionedRowsetCanonicalBlob() {
        return ArrayUtils.addAll(makeByteArray(
                // row count
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        ), makeVersionedRowCanonicalBlob());
    }

    public static byte[] makeVersionedRowCanonicalBlob() {
        return makeByteArray(
                0x0c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89,
                0x67, 0x45, 0x23, 0x01, 0xe8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x03, 0x00, 0x00,
                0x00, 0x00, 0x00, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0xe9, 0x03, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x04, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc,
                0xfe, 0xea, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0xeb, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
                0x00, 0x05, 0x01, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40, 0xec, 0x03,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x2d, 0x44,
                0x54, 0xfb, 0x21, 0x09, 0x40, 0xed, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x06, 0x01,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xee, 0x03, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x09, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0xef, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x10, 0x01, 0x01, 0x00, 0x00,
                0x00, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x0b, 0x00, 0x10, 0x00, 0x01, 0x00, 0x00, 0x00, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf1,
                0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x11, 0x01, 0x04, 0x00, 0x00, 0x00, 0x01, 0x04,
                0x7b, 0x7d, 0x00, 0x00, 0x00, 0x00, 0xf2, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x11,
                0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x04, 0x7b, 0x7d, 0x00, 0x00, 0x00, 0x00, 0xf3, 0x03, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00);
    }

    public static List<WireColumnSchema> extractSchemaData(UnversionedRow row, ColumnValueType nullType) {
        List<WireColumnSchema> result = new ArrayList<>();
        for (UnversionedValue value : row.getValues()) {
            result.add(new WireColumnSchema(
                    value.getId(),
                    value.getType() != ColumnValueType.NULL ? value.getType() : nullType,
                    value.isAggregate()));
        }
        return result;
    }

    public static String hexString(byte[] data) {
        return Hex.encode(data);
    }

    public static Matcher<byte[]> dataEquals(byte[] canonical) {
        return new TypeSafeMatcher<byte[]>() {
            @Override
            protected boolean matchesSafely(byte[] item) {
                if (item.length != canonical.length) {
                    return false;
                }
                for (int i = 0; i < canonical.length; i++) {
                    // 0xcf marks garbage due to alignment
                    if (canonical[i] != (byte) 0xcf) {
                        if (item[i] != canonical[i]) {
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(hexString(canonical));
            }

            @Override
            protected void describeMismatchSafely(byte[] item, Description mismatchDescription) {
                mismatchDescription.appendText("was ").appendText(hexString(item));
            }
        };
    }

    interface TriConsumer<K, V, T> {
        void accept(K value1, V value2, T value3);
    }
}
