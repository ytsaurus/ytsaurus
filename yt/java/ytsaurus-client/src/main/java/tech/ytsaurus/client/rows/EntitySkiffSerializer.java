package tech.ytsaurus.client.rows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.Decimal;
import tech.ytsaurus.core.rows.YsonSerializable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.core.utils.ClassUtils;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.skiff.SkiffParser;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.SkiffSerializer;
import tech.ytsaurus.typeinfo.DecimalType;
import tech.ytsaurus.typeinfo.StructType;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.BufferReference;
import tech.ytsaurus.yson.ClosableYsonConsumer;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

import static tech.ytsaurus.client.rows.TiTypeUtil.isSimpleType;
import static tech.ytsaurus.client.rows.TiTypeUtil.tableSchemaToStructTiType;
import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.boxArray;
import static tech.ytsaurus.core.utils.ClassUtils.castIntToActualType;
import static tech.ytsaurus.core.utils.ClassUtils.castLongToActualType;
import static tech.ytsaurus.core.utils.ClassUtils.castShortToActualType;
import static tech.ytsaurus.core.utils.ClassUtils.castToType;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getConstructorOrDefaultFor;
import static tech.ytsaurus.core.utils.ClassUtils.getInstanceWithoutArguments;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeDescription;
import static tech.ytsaurus.core.utils.ClassUtils.isAssignableToInt;
import static tech.ytsaurus.core.utils.ClassUtils.isAssignableToLong;
import static tech.ytsaurus.core.utils.ClassUtils.isAssignableToShort;
import static tech.ytsaurus.core.utils.ClassUtils.setFieldsAccessibleToTrue;
import static tech.ytsaurus.core.utils.ClassUtils.unboxArray;

@NonNullApi
@NonNullFields
public class EntitySkiffSerializer<T> {
    private static final byte ZERO_TAG = 0x00;
    private static final byte ONE_TAG = 0x01;
    private static final byte END_TAG = (byte) 0xFF;
    private final Class<T> entityClass;
    private final Map<Class<?>, List<EntityFieldDescr>> entityFieldsMap = new HashMap<>();
    private final Map<Class<?>, Constructor<?>> objectConstructorMap = new HashMap<>();
    private @Nullable TableSchema entityTableSchema;
    private @Nullable TiType entityTiType;
    private @Nullable SkiffSchema skiffSchema;
    private @Nullable Format format;

    public EntitySkiffSerializer(Class<T> entityClass) {
        if (!anyOfAnnotationsPresent(entityClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }
        this.entityClass = entityClass;
        try {
            this.entityTableSchema = EntityTableSchemaCreator.create(entityClass);
            this.entityTiType = tableSchemaToStructTiType(entityTableSchema);
            this.skiffSchema = SchemaConverter.toSkiffSchema(entityTableSchema);
            this.format = Format.skiff(skiffSchema, 1);
        } catch (EntityTableSchemaCreator.PrecisionAndScaleNotSpecifiedException ignored) {
        }

        entityFieldsMap.put(entityClass, getEntityFieldDescrs(entityClass));
    }

    public Optional<Format> getFormat() {
        return Optional.ofNullable(format);
    }

    public Optional<TableSchema> getEntityTableSchema() {
        return Optional.ofNullable(entityTableSchema);
    }

    public void setTableSchema(TableSchema tableSchema) {
        this.entityTableSchema = EntityTableSchemaCreator.create(entityClass, tableSchema);
        this.entityTiType = tableSchemaToStructTiType(entityTableSchema);
        this.skiffSchema = SchemaConverter.toSkiffSchema(entityTableSchema);
        this.format = Format.skiff(skiffSchema, 1);
    }

    public byte[] serialize(T object) {
        if (entityTiType == null) {
            throwNoSchemaException();
        }

        ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        serialize(object, new SkiffSerializer(byteOS));
        try {
            byteOS.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOS.toByteArray();
    }

    public void serialize(T object, SkiffSerializer serializer) {
        if (entityTiType == null) {
            throwNoSchemaException();
        }
        serializeEntity(object, entityTiType, serializer);
    }

    public Optional<T> deserialize(byte[] objectBytes) {
        return deserialize(new SkiffParser(new ByteArrayInputStream(objectBytes)));
    }

    public Optional<T> deserialize(SkiffParser parser) {
        if (entityTiType == null) {
            throwNoSchemaException();
        }
        return Optional.ofNullable(
                deserializeObject(entityClass, entityTiType, List.of(), parser)
        );
    }

    private <ObjectType> void serializeObject(
            @Nullable ObjectType object,
            TiType tiType,
            SkiffSerializer serializer
    ) {
        boolean isNullable = tiType.isOptional();
        if (object == null) {
            if (!isNullable) {
                throw new NullPointerException(
                        String.format("Field '%s' is non nullable", tiType));
            }
            serializer.serializeByte(ZERO_TAG);
            return;
        }
        if (isNullable) {
            serializer.serializeByte(ONE_TAG);
            tiType = tiType.asOptional().getItem();
        }

        if (isSimpleType(tiType)) {
            serializeSimpleType(object, tiType, serializer);
            return;
        }
        if (tiType.isDecimal()) {
            if (!object.getClass().equals(BigDecimal.class)) {
                throwInvalidSchemeException(null);
            }
            serializeDecimal((BigDecimal) object, tiType.asDecimal(), serializer);
            return;
        }

        serializeComplexObject(object, tiType, serializer);
    }

    private void serializeDecimal(
            BigDecimal value,
            DecimalType decimalType,
            SkiffSerializer serializer
    ) {
        byte[] dataInBigEndian = Decimal.textToBinary(
                value.toPlainString(),
                decimalType.getPrecision(),
                decimalType.getScale()
        );
        dataInBigEndian[0] = (byte) (dataInBigEndian[0] ^ (0x1 << 7));

        for (int i = dataInBigEndian.length - 1; i >= 0; i--) {
            serializer.serializeByte(dataInBigEndian[i]);
        }
    }

    private <SimpleType> void serializeSimpleType(
            SimpleType object,
            TiType tiType,
            SkiffSerializer serializer
    ) {
        try {
            if (tiType.isInt8()) {
                serializer.serializeByte((byte) object);
            } else if (tiType.isInt16()) {
                serializeInt16(object, serializer);
            } else if (tiType.isInt32()) {
                serializeInt32(object, serializer);
            } else if (tiType.isInt64()) {
                serializeInt64(object, serializer);
            } else if (tiType.isUint8()) {
                serializer.serializeUint8((long) object);
            } else if (tiType.isUint16()) {
                serializer.serializeUint16((long) object);
            } else if (tiType.isUint32()) {
                serializer.serializeUint32((long) object);
            } else if (tiType.isUint64()) {
                serializer.serializeUint64((long) object);
            } else if (tiType.isDouble()) {
                serializer.serializeDouble((double) object);
            } else if (tiType.isBool()) {
                serializer.serializeBoolean((boolean) object);
            } else if (tiType.isUtf8()) {
                serializeUtf8(object, serializer);
            } else if (tiType.isString()) {
                serializeString(object, serializer);
            } else if (tiType.isUuid()) {
                serializer.serializeGuid((GUID) object);
            } else if (tiType.isTimestamp()) {
                serializer.serializeTimestamp((Instant) object);
            } else if (tiType.isYson()) {
                serializeYson(object, serializer);
            } else {
                throw new IllegalArgumentException(
                        String.format("Type '%s' is not supported", tiType));
            }
            return;
        } catch (ClassCastException e) {
            throwInvalidSchemeException(e);
        }
        throw new IllegalStateException();
    }

    private <Int16Type> void serializeInt16(
            Int16Type object,
            SkiffSerializer serializer
    ) {
        if (isAssignableToShort(object.getClass())) {
            serializer.serializeShort(((Number) object).shortValue());
            return;
        }
        throwIncorrectFieldTypeException("int16", object.getClass());
    }

    private <Int32Type> void serializeInt32(
            Int32Type object,
            SkiffSerializer serializer
    ) {
        if (isAssignableToInt(object.getClass())) {
            serializer.serializeInt(((Number) object).intValue());
            return;
        }
        throwIncorrectFieldTypeException("int32", object.getClass());
    }

    private <Int64Type> void serializeInt64(
            Int64Type object,
            SkiffSerializer serializer
    ) {
        long l = 0;
        if (isAssignableToLong(object.getClass())) {
            l = ((Number) object).longValue();
        } else if (object.getClass().equals(Instant.class)) {
            l = ((Instant) object).toEpochMilli();
        } else {
            throwIncorrectFieldTypeException("int64", object.getClass());
        }
        serializer.serializeLong(l);
    }

    private <Utf8Type> void serializeUtf8(
            Utf8Type object,
            SkiffSerializer serializer
    ) {
        String str = null;
        if (object.getClass().equals(String.class)) {
            str = (String) object;
        } else if (Enum.class.isAssignableFrom(object.getClass())) {
            str = ((Enum<?>) object).name();
        } else {
            throwIncorrectFieldTypeException("utf8", object.getClass());
        }
        serializer.serializeUtf8(str);
    }

    private <StringType> void serializeString(
            StringType object,
            SkiffSerializer serializer
    ) {
        if (object.getClass().equals(byte[].class)) {
            serializer.serializeString((byte[]) object);
        } else if (object.getClass().equals(String.class) || Enum.class.isAssignableFrom(object.getClass())) {
            serializeUtf8(object, serializer);
        } else {
            throwIncorrectFieldTypeException("string", object.getClass());
        }
    }

    private <YsonType> void serializeYson(
            YsonType object,
            SkiffSerializer serializer
    ) {
        var byteOutputStreamForYson = new ByteArrayOutputStream();
        ClosableYsonConsumer consumer = YTreeBinarySerializer.getSerializer(byteOutputStreamForYson);
        if (YTreeNode.class.isAssignableFrom(object.getClass())) {
            YTreeNodeUtils.walk((YTreeNode) object, consumer, true);
        } else if (YsonSerializable.class.isAssignableFrom(object.getClass())) {
            ((YsonSerializable) object).serialize(consumer);
        } else {
            throwIncorrectFieldTypeException("yson", object.getClass());
        }
        consumer.close();
        byte[] bytes = byteOutputStreamForYson.toByteArray();
        serializer.serializeString(bytes);
    }

    private <ObjectType> void serializeComplexObject(
            ObjectType object,
            TiType objectTiType,
            SkiffSerializer serializer
    ) {
        final Class<?> clazz = object.getClass();
        if (Collection.class.isAssignableFrom(clazz)) {
            serializeCollection(object, objectTiType, serializer);
            return;
        }
        if (Map.class.isAssignableFrom(clazz)) {
            serializeMap(object, objectTiType, serializer);
            return;
        }
        if (clazz.isArray()) {
            serializeArray(object, objectTiType, serializer);
            return;
        }

        serializeEntity(object, objectTiType, serializer);
    }

    private static List<EntityFieldDescr> getEntityFieldDescrs(Class<?> entityClass) {
        var declaredFields = getAllDeclaredFields(entityClass);
        setFieldsAccessibleToTrue(declaredFields);
        return EntityFieldDescr.of(declaredFields);
    }

    private <ObjectType> void serializeEntity(
            ObjectType object,
            TiType structTiType,
            SkiffSerializer serializer
    ) {
        if (!structTiType.isStruct()) {
            throwInvalidSchemeException(null);
        }

        serializeEntity(object, structTiType.asStruct().getMembers().iterator(), serializer);
    }

    private <ObjectType> void serializeEntity(
            ObjectType object,
            Iterator<StructType.Member> structMembersIterator,
            SkiffSerializer serializer
    ) {
        if (!structMembersIterator.hasNext()) {
            throwInvalidSchemeException(null);
        }

        var fieldDescriptions = entityFieldsMap.computeIfAbsent(
                object.getClass(), EntitySkiffSerializer::getEntityFieldDescrs);
        for (var fieldDescr : fieldDescriptions) {
            if (fieldDescr.isTransient()) {
                continue;
            }
            try {
                if (fieldDescr.isEmbeddable()) {
                    serializeEntity(fieldDescr.getField().get(object), structMembersIterator, serializer);
                } else {
                    serializeObject(
                            fieldDescr.getField().get(object),
                            structMembersIterator.next().getType(),
                            serializer
                    );
                }
            } catch (IllegalAccessException e) {
                throwInvalidSchemeException(e);
            }
        }

    }

    private <CollectionType, ElemType> void serializeCollection(
            CollectionType object,
            TiType tiType,
            SkiffSerializer serializer
    ) {
        if (!tiType.isList()) {
            throwInvalidSchemeException(null);
        }

        Collection<ElemType> collection = castToType(object);
        var elemTiType = tiType.asList().getItem();
        for (var elem : collection) {
            serializer.serializeByte(ZERO_TAG);
            serializeObject(elem, elemTiType, serializer);
        }
        serializer.serializeByte(END_TAG);
    }

    private <MapType, KeyType, ValueType> void serializeMap(
            MapType object,
            TiType tiType,
            SkiffSerializer serializer
    ) {
        if (!tiType.isDict()) {
            throwInvalidSchemeException(null);
        }

        Map<KeyType, ValueType> map = castToType(object);
        var keyTiType = tiType.asDict().getKey();
        var valueTiType = tiType.asDict().getValue();
        for (var entry : map.entrySet()) {
            serializer.serializeByte(ZERO_TAG);
            serializeObject(entry.getKey(), keyTiType, serializer);
            serializeObject(entry.getValue(), valueTiType, serializer);
        }
        serializer.serializeByte(END_TAG);
    }

    private <ArrayType, ElemType> void serializeArray(
            ArrayType object,
            TiType tiType,
            SkiffSerializer serializer
    ) {
        if (!tiType.isList()) {
            throwInvalidSchemeException(null);
        }

        Class<?> elementClass = object.getClass().getComponentType();
        ElemType[] list = elementClass.isPrimitive() ?
                boxArray(object, elementClass) :
                castToType(object);
        var elemTiType = tiType.asList().getItem();
        for (var elem : list) {
            serializer.serializeByte(ZERO_TAG);
            serializeObject(elem, elemTiType, serializer);
        }
        serializer.serializeByte(END_TAG);
    }

    private <ObjectType> @Nullable ObjectType deserializeObject(
            Class<ObjectType> clazz,
            TiType tiType,
            List<Type> genericTypeParameters,
            SkiffParser parser
    ) {
        if (tiType.isOptional()) {
            if (parser.parseVariant8Tag() == ZERO_TAG) {
                return null;
            }
            tiType = tiType.asOptional().getItem();
        }

        if (isSimpleType(tiType)) {
            return deserializeSimpleType(clazz, tiType, parser);
        }
        if (tiType.isDecimal()) {
            if (!clazz.equals(BigDecimal.class)) {
                throwInvalidSchemeException(null);
            }
            return castToType(deserializeDecimal(tiType.asDecimal(), parser));
        }

        return deserializeComplexObject(clazz, tiType, genericTypeParameters, parser);
    }

    private BigDecimal deserializeDecimal(DecimalType decimalType, SkiffParser parser) {
        byte[] binaryDecimal = parser.getDataInBigEndian(
                getBinaryDecimalLength(decimalType)
        );
        binaryDecimal[0] = (byte) (binaryDecimal[0] ^ (0x1 << 7));
        String textDecimal = Decimal.binaryToText(
                binaryDecimal,
                decimalType.getPrecision(),
                decimalType.getScale()
        );

        if (textDecimal.equals("nan") || textDecimal.equals("inf") || textDecimal.equals("-inf")) {
            throw new IllegalArgumentException(String.format(
                    "YT Decimal value '%s' is not supported by Java BigDecimal", textDecimal));
        }

        return new BigDecimal(textDecimal);
    }

    private int getBinaryDecimalLength(TiType tiType) {
        int precision = tiType.asDecimal().getPrecision();
        if (precision <= 9) {
            return 4;
        }
        if (precision <= 18) {
            return 8;
        }
        return 16;
    }

    private <ObjectType> ObjectType deserializeComplexObject(
            Class<ObjectType> clazz,
            TiType tiType,
            List<Type> genericTypeParameters,
            SkiffParser parser
    ) {
        if (List.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    clazz,
                    genericTypeParameters.get(0),
                    tiType,
                    ArrayList.class,
                    parser
            ));
        }
        if (Set.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    clazz,
                    genericTypeParameters.get(0),
                    tiType,
                    HashSet.class,
                    parser
            ));
        }
        if (Queue.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    clazz,
                    genericTypeParameters.get(0),
                    tiType,
                    LinkedList.class,
                    parser
            ));
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return castToType(deserializeMap(
                    clazz,
                    genericTypeParameters.get(0),
                    genericTypeParameters.get(1),
                    tiType,
                    parser
            ));
        }
        if (clazz.isArray()) {
            return deserializeArray(clazz, tiType, parser);
        }

        return deserializeEntity(clazz, tiType, parser);
    }

    private <ObjectType> ObjectType deserializeEntity(
            Class<ObjectType> clazz,
            TiType tiType,
            SkiffParser parser
    ) {
        if (!tiType.isStruct()) {
            throwInvalidSchemeException(null);
        }

        return deserializeEntity(clazz, tiType.asStruct().getMembers().iterator(), parser);
    }

    private <ObjectType> ObjectType deserializeEntity(
            Class<ObjectType> clazz,
            Iterator<StructType.Member> structMembersIterator,
            SkiffParser parser
    ) {
        if (!structMembersIterator.hasNext()) {
            throwInvalidSchemeException(null);
        }

        var defaultConstructor = objectConstructorMap.computeIfAbsent(clazz, ClassUtils::getEmptyConstructor);

        ObjectType object = getInstanceWithoutArguments(defaultConstructor);

        var fieldDescriptions = entityFieldsMap.computeIfAbsent(clazz, EntitySkiffSerializer::getEntityFieldDescrs);
        for (var fieldDescr : fieldDescriptions) {
            if (fieldDescr.isTransient()) {
                continue;
            }
            try {
                Object value;
                if (fieldDescr.isEmbeddable()) {
                    value = deserializeEntity(fieldDescr.getField().getType(), structMembersIterator, parser);
                } else {
                    value = deserializeObject(
                            castToType(fieldDescr.getField().getType()),
                            structMembersIterator.next().getType(),
                            fieldDescr.getTypeParameters(),
                            parser
                    );
                }
                fieldDescr.getField().set(object, value);
            } catch (IllegalAccessException e) {
                throwInvalidSchemeException(e);
            }
        }
        return object;
    }

    private <ElemType> Collection<ElemType> deserializeCollection(
            Class<?> clazz,
            Type elementType,
            TiType tiType,
            Class<?> defaultClass,
            SkiffParser parser
    ) {
        if (!tiType.isList()) {
            throwInvalidSchemeException(null);
        }

        var collectionConstructor = objectConstructorMap.computeIfAbsent(
                clazz,
                getConstructorOrDefaultFor(defaultClass)
        );

        Collection<ElemType> collection = getInstanceWithoutArguments(collectionConstructor);
        var elementTypeDescr = getTypeDescription(elementType);
        while (parser.parseInt8() != END_TAG) {
            collection.add(castToType(
                            deserializeObject(
                                    elementTypeDescr.getTypeClass(),
                                    tiType.asList().getItem(),
                                    elementTypeDescr.getTypeParameters(),
                                    parser
                            )
                    )
            );
        }
        return collection;
    }


    private <KeyType, ValueType> Map<KeyType, ValueType> deserializeMap(
            Class<?> clazz,
            Type keyType,
            Type valueType,
            TiType tiType,
            SkiffParser parser
    ) {
        if (!tiType.isDict()) {
            throwInvalidSchemeException(null);
        }

        var mapConstructor = objectConstructorMap.computeIfAbsent(
                clazz,
                getConstructorOrDefaultFor(HashMap.class)
        );

        Map<KeyType, ValueType> map = getInstanceWithoutArguments(mapConstructor);
        var keyTypeDescr = getTypeDescription(keyType);
        var valueTypeDescr = getTypeDescription(valueType);
        while (parser.parseInt8() != END_TAG) {
            map.put(castToType(deserializeObject(
                            keyTypeDescr.getTypeClass(),
                            tiType.asDict().getKey(),
                            keyTypeDescr.getTypeParameters(),
                            parser
                    )),
                    castToType(deserializeObject(
                            valueTypeDescr.getTypeClass(),
                            tiType.asDict().getValue(),
                            valueTypeDescr.getTypeParameters(),
                            parser
                    ))
            );
        }
        return map;
    }

    private <ArrayType, ElemType> ArrayType deserializeArray(
            Class<?> clazz,
            TiType tiType,
            SkiffParser parser
    ) {
        var elementClass = clazz.getComponentType();
        List<ElemType> list = castToType(deserializeCollection(
                clazz, elementClass, tiType, ArrayList.class, parser
        ));
        Object arrayObject = Array.newInstance(elementClass, list.size());
        ElemType[] array = elementClass.isPrimitive() ?
                boxArray(arrayObject, elementClass) :
                castToType(arrayObject);
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        if (elementClass.isPrimitive()) {
            return unboxArray(array, array.getClass().getComponentType());
        }
        return castToType(array);
    }

    private <SimpleType> SimpleType deserializeSimpleType(
            Class<SimpleType> clazz,
            TiType tiType,
            SkiffParser parser
    ) {
        try {
            if (tiType.isInt8()) {
                return castToType(parser.parseInt8());
            } else if (tiType.isInt16()) {
                return deserializeInt16(clazz, parser);
            } else if (tiType.isInt32()) {
                return deserializeInt32(clazz, parser);
            } else if (tiType.isInt64()) {
                return deserializeInt64(clazz, parser);
            } else if (tiType.isUint8()) {
                return castToType(parser.parseUint8());
            } else if (tiType.isUint16()) {
                return castToType(parser.parseUint16());
            } else if (tiType.isUint32()) {
                return castToType(parser.parseUint32());
            } else if (tiType.isUint64()) {
                return castToType(parser.parseUint64());
            } else if (tiType.isDouble()) {
                return castToType(parser.parseDouble());
            } else if (tiType.isBool()) {
                return castToType(parser.parseBoolean());
            } else if (tiType.isUtf8()) {
                return castToType(deserializeUtf8(clazz, parser));
            } else if (tiType.isString()) {
                return deserializeString(clazz, parser);
            } else if (tiType.isUuid()) {
                return castToType(deserializeGuid(parser));
            } else if (tiType.isTimestamp()) {
                return castToType(deserializeTimestamp(parser));
            } else if (tiType.isYson()) {
                return castToType(deserializeYson(clazz, parser));
            }
            throw new IllegalArgumentException(
                    String.format("Type '%s' is not supported", tiType));
        } catch (ClassCastException e) {
            throwInvalidSchemeException(e);
        }
        throw new IllegalStateException();
    }

    private <Int16Type> Int16Type deserializeInt16(Class<Int16Type> clazz, SkiffParser parser) {
        short int16 = parser.parseInt16();
        if (isAssignableToShort(clazz)) {
            return castShortToActualType(int16, clazz);
        }
        throwIncorrectFieldTypeException("int16", clazz);
        return null;
    }

    private <Int32Type> Int32Type deserializeInt32(Class<Int32Type> clazz, SkiffParser parser) {
        int int32 = parser.parseInt32();
        if (isAssignableToInt(clazz)) {
            return castIntToActualType(int32, clazz);
        }
        throwIncorrectFieldTypeException("int32", clazz);
        return null;
    }

    private <Int64Type> Int64Type deserializeInt64(Class<Int64Type> clazz, SkiffParser parser) {
        long int64 = parser.parseInt64();
        if (isAssignableToLong(clazz)) {
            return castLongToActualType(int64, clazz);
        }
        if (clazz.equals(Instant.class)) {
            return castToType(Instant.ofEpochMilli(int64));
        }
        throwIncorrectFieldTypeException("int64", clazz);
        return null;
    }

    private <Utf8Type> Utf8Type deserializeUtf8(Class<Utf8Type> clazz, SkiffParser parser) {
        BufferReference ref = parser.parseString32();
        String str = new String(ref.getBuffer(), ref.getOffset(), ref.getLength(), StandardCharsets.UTF_8);
        if (clazz.equals(String.class)) {
            return castToType(str);
        }
        if (Enum.class.isAssignableFrom(clazz)) {
            return castToType(deserializeEnum(clazz, str));
        }
        throwIncorrectFieldTypeException("utf8", clazz);
        return null;
    }

    private <E extends Enum<E>> E deserializeEnum(Class<?> clazz, String value) {
        return Enum.valueOf(castToType(clazz), value);
    }

    private <StringType> StringType deserializeString(Class<StringType> clazz, SkiffParser parser) {
        if (clazz.equals(byte[].class)) {
            BufferReference ref = parser.parseString32();
            return castToType(
                    Arrays.copyOfRange(ref.getBuffer(), ref.getOffset(), ref.getOffset() + ref.getLength())
            );
        }
        if (clazz.equals(String.class) || Enum.class.isAssignableFrom(clazz)) {
            return deserializeUtf8(clazz, parser);
        }
        throwIncorrectFieldTypeException("string", clazz);
        return null;
    }

    private GUID deserializeGuid(SkiffParser parser) {
        if (parser.parseInt32() != 16) {
            throw new IllegalArgumentException("Length of UUID must be exactly 16 bytes");
        }
        return new GUID(parser.parseInt64(), parser.parseInt64());
    }

    private Instant deserializeTimestamp(SkiffParser parser) {
        return Instant.ofEpochMilli(parser.parseUint64());
    }

    private <YsonType> YsonType deserializeYson(Class<YsonType> clazz, SkiffParser parser) {
        BufferReference ref = parser.parseYson32();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(ref.getBuffer(), ref.getOffset(), ref.getLength());
        YTreeNode node = YTreeBinarySerializer.deserialize(inputStream);
        if (YTreeNode.class.isAssignableFrom(clazz)) {
            return castToType(node);
        }
        if (YsonSerializable.class.isAssignableFrom(clazz)) {
            var constructor = objectConstructorMap.computeIfAbsent(clazz, ClassUtils::getEmptyConstructor);
            YsonSerializable ysonSerializable = getInstanceWithoutArguments(constructor);
            ysonSerializable.deserialize(node);
            return castToType(ysonSerializable);
        }
        throwIncorrectFieldTypeException("yson", clazz);
        return null;
    }

    private void throwInvalidSchemeException(@Nullable Exception e) {
        throw new IllegalStateException("Scheme does not correspond to object", e);
    }

    private void throwIncorrectFieldTypeException(String typeName, Class<?> clazz) {
        throw new RuntimeException(
                String.format("Incorrect field type for '%s': %s", typeName, clazz.getCanonicalName())
        );
    }

    private void throwNoSchemaException() {
        throw new IllegalStateException("Cannot create or get table schema");
    }
}
