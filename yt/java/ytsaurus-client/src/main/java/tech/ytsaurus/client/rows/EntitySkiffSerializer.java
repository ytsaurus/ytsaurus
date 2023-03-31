package tech.ytsaurus.client.rows;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.core.common.Decimal;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.skiff.SkiffParser;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.typeinfo.DecimalType;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.BufferReference;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

import static tech.ytsaurus.client.rows.TiTypeUtil.isSimpleType;
import static tech.ytsaurus.client.rows.TiTypeUtil.tableSchemaToStructTiType;
import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.boxArray;
import static tech.ytsaurus.core.utils.ClassUtils.castToType;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getConstructorOrDefaultFor;
import static tech.ytsaurus.core.utils.ClassUtils.getInstanceWithoutArguments;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeDescription;
import static tech.ytsaurus.core.utils.ClassUtils.setFieldsAccessibleToTrue;
import static tech.ytsaurus.core.utils.ClassUtils.unboxArray;

public class EntitySkiffSerializer<T> {
    private static final byte ZERO_TAG = 0x00;
    private static final byte ONE_TAG = 0x01;
    private static final byte END_TAG = (byte) 0xFF;
    private final Class<T> entityClass;
    private @Nullable TableSchema entityTableSchema;
    private @Nullable TiType entityTiType;
    private @Nullable SkiffSchema skiffSchema;
    private @Nullable Format format;
    private final List<EntityFieldDescr> entityFieldDescriptions;
    private final Map<Class<?>, List<EntityFieldDescr>> entityFieldsMap = new HashMap<>();
    private final Map<Class<?>, Constructor<?>> complexObjectConstructorMap = new HashMap<>();

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

        var declaredFields = getAllDeclaredFields(entityClass);
        this.entityFieldDescriptions = EntityFieldDescr.of(declaredFields);
        setFieldsAccessibleToTrue(declaredFields);
        entityFieldsMap.put(entityClass, entityFieldDescriptions);
    }

    public Optional<Format> getFormat() {
        return Optional.ofNullable(format);
    }

    public void setTableSchema(TableSchema tableSchema) {
        if (entityTableSchema == null) {
            this.entityTableSchema = tableSchema;
            this.entityTiType = tableSchemaToStructTiType(tableSchema);
            this.skiffSchema = SchemaConverter.toSkiffSchema(tableSchema);
            this.format = Format.skiff(skiffSchema, 1);
            return;
        }
        if (!tableSchema.equals(entityTableSchema)) {
            throw new MismatchEntityAndTableSchemaException(
                    String.format(
                            "Entity schema does not match received table schema:\nEntity schema: %s\nTable schema: %s",
                            entityTableSchema, tableSchema
                    )
            );
        }
    }

    public byte[] serialize(T object) {
        if (entityTiType == null) {
            throwNoSchemaException();
        }

        ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        serializeEntity(object, entityTiType, entityFieldDescriptions, byteOS);
        try {
            byteOS.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOS.toByteArray();
    }

    public void serialize(T object, BufferedOutputStream outputStream) {
        if (entityTiType == null) {
            throwNoSchemaException();
        }

        serializeEntity(object, entityTiType, entityFieldDescriptions, outputStream);
    }

    public Optional<T> deserialize(byte[] objectBytes) {
        return deserialize(new SkiffParser(new ByteArrayInputStream(objectBytes)));
    }

    public Optional<T> deserialize(SkiffParser parser) {
        if (entityTiType == null) {
            throwNoSchemaException();
        }
        return Optional.ofNullable(deserializeObject(
                parser, entityClass, entityTiType, List.of()
        ));
    }

    private <ObjectType> void serializeObject(@Nullable ObjectType object,
                                              TiType tiType,
                                              OutputStream byteOS) throws IOException {
        boolean isNullable = tiType.isOptional();
        if (object == null) {
            if (!isNullable) {
                throw new NullPointerException(
                        String.format("Field '%s' is non nullable", tiType));
            }
            byteOS.write(ZERO_TAG);
            return;
        }
        if (isNullable) {
            byteOS.write(ONE_TAG);
            tiType = tiType.asOptional().getItem();
        }

        Class<?> clazz = object.getClass();
        if (isSimpleType(tiType)) {
            serializeSimpleType(object, tiType, byteOS);
            return;
        }
        if (tiType.isDecimal()) {
            if (!object.getClass().equals(BigDecimal.class)) {
                throwInvalidSchemeException(null);
            }
            serializeDecimal((BigDecimal) object, tiType.asDecimal(), byteOS);
            return;
        }

        serializeComplexObject(object, tiType, byteOS, clazz);
    }

    private void serializeDecimal(BigDecimal value,
                                  DecimalType decimalType,
                                  OutputStream byteOS) throws IOException {
        byte[] dataInBigEndian = Decimal.textToBinary(
                value.toString(),
                decimalType.getPrecision(),
                decimalType.getScale()
        );
        dataInBigEndian[0] = (byte) (dataInBigEndian[0] ^ (0x1 << 7));

        for (int i = dataInBigEndian.length - 1; i >= 0; i--) {
            byteOS.write(dataInBigEndian[i]);
        }
    }

    private <SimpleType> void serializeSimpleType(SimpleType object,
                                                  TiType tiType,
                                                  OutputStream byteOS) {
        try {
            if (tiType.isInt8()) {
                serializeByte((byte) object, byteOS);
            } else if (tiType.isInt16()) {
                serializeShort((short) object, byteOS);
            } else if (tiType.isInt32()) {
                serializeInt((int) object, byteOS);
            } else if (tiType.isInt64()) {
                serializeLong((long) object, byteOS);
            } else if (tiType.isDouble()) {
                serializeDouble((double) object, byteOS);
            } else if (tiType.isBool()) {
                serializeBoolean((boolean) object, byteOS);
            } else if (tiType.isString()) {
                serializeString((String) object, byteOS);
            } else if (tiType.isYson()) {
                serializeYson((YTreeNode) object, byteOS);
            } else {
                throw new IllegalArgumentException(
                        String.format("Type '%s' is not supported", tiType));
            }
            return;
        } catch (ClassCastException | IOException e) {
            throwInvalidSchemeException(e);
        }
        throw new IllegalStateException();
    }

    private <ObjectType> void serializeComplexObject(ObjectType object,
                                                     TiType objectTiType,
                                                     OutputStream byteOS,
                                                     Class<?> clazz) throws IOException {
        if (Collection.class.isAssignableFrom(object.getClass())) {
            serializeCollection(object, objectTiType, byteOS);
            return;
        }
        if (Map.class.isAssignableFrom(object.getClass())) {
            serializeMap(object, objectTiType, byteOS);
            return;
        }
        if (clazz.isArray()) {
            serializeArray(object, objectTiType, byteOS);
            return;
        }

        var entityFields = entityFieldsMap.computeIfAbsent(clazz, entityClass -> {
            var declaredFields = getAllDeclaredFields(entityClass);
            setFieldsAccessibleToTrue(declaredFields);
            return EntityFieldDescr.of(declaredFields);
        });
        serializeEntity(object, objectTiType, entityFields, byteOS);
    }

    private <ObjectType> void serializeEntity(ObjectType object,
                                              TiType structTiType,
                                              List<EntityFieldDescr> fieldDescriptions,
                                              OutputStream byteOS) {
        if (!structTiType.isStruct()) {
            throwInvalidSchemeException(null);
        }

        int indexInSchema = 0;
        for (var fieldDescr : fieldDescriptions) {
            if (fieldDescr.isTransient()) {
                continue;
            }
            try {
                serializeObject(
                        fieldDescr.getField().get(object),
                        structTiType.asStruct().getMembers()
                                .get(indexInSchema).getType(),
                        byteOS);
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException | IOException e) {
                throwInvalidSchemeException(e);
            }
        }
    }

    private <CollectionType, ElemType> void serializeCollection(CollectionType object,
                                                                TiType tiType,
                                                                OutputStream byteOS) throws IOException {
        if (!tiType.isList()) {
            throwInvalidSchemeException(null);
        }

        Collection<ElemType> collection = castToType(object);
        var elemTiType = tiType.asList().getItem();
        for (var elem : collection) {
            byteOS.write(ZERO_TAG);
            serializeObject(elem, elemTiType, byteOS);
        }
        serializeByte(END_TAG, byteOS);
    }

    private <ListType, KeyType, ValueType> void serializeMap(ListType object,
                                                             TiType tiType,
                                                             OutputStream byteOS) throws IOException {
        if (!tiType.isDict()) {
            throwInvalidSchemeException(null);
        }

        Map<KeyType, ValueType> map = castToType(object);
        var keyTiType = tiType.asDict().getKey();
        var valueTiType = tiType.asDict().getValue();
        for (var entry : map.entrySet()) {
            byteOS.write(ZERO_TAG);
            serializeObject(entry.getKey(), keyTiType, byteOS);
            serializeObject(entry.getValue(), valueTiType, byteOS);
        }
        serializeByte(END_TAG, byteOS);
    }

    private <ArrayType, ElemType> void serializeArray(ArrayType object,
                                                      TiType tiType,
                                                      OutputStream byteOS) throws IOException {
        if (!tiType.isList()) {
            throwInvalidSchemeException(null);
        }

        Class<?> elementClass = object.getClass().getComponentType();
        ElemType[] list = elementClass.isPrimitive() ?
                boxArray(object, elementClass) :
                castToType(object);
        var elemTiType = tiType.asList().getItem();
        for (var elem : list) {
            byteOS.write(ZERO_TAG);
            serializeObject(elem, elemTiType, byteOS);
        }
        serializeByte(END_TAG, byteOS);
    }

    private void serializeByte(byte number, OutputStream byteOS) throws IOException {
        byteOS.write(number);
    }

    private void serializeShort(short number, OutputStream byteOS) throws IOException {
        byteOS.write((number & 0xFF));
        byteOS.write((number >> 8) & 0xFF);
    }

    private void serializeInt(int number, OutputStream byteOS) throws IOException {
        byteOS.write((number & 0xFF));
        byteOS.write((number >> 8) & 0xFF);
        byteOS.write((number >> 16) & 0xFF);
        byteOS.write((number >> 24) & 0xFF);
    }

    private void serializeLong(long number, OutputStream byteOS) throws IOException {
        byteOS.write((int) (number & 0xFF));
        byteOS.write((int) ((number >> 8) & 0xFF));
        byteOS.write((int) ((number >> 16) & 0xFF));
        byteOS.write((int) ((number >> 24) & 0xFF));
        byteOS.write((int) ((number >> 32) & 0xFF));
        byteOS.write((int) ((number >> 40) & 0xFF));
        byteOS.write((int) ((number >> 48) & 0xFF));
        byteOS.write((int) ((number >> 56) & 0xFF));
    }

    private void serializeDouble(double number, OutputStream byteOS) throws IOException {
        byteOS.write(ByteBuffer
                .allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                .putDouble(number)
                .array());
    }

    private void serializeBoolean(boolean bool, OutputStream byteOS) throws IOException {
        byteOS.write(bool ? 1 : 0);
    }

    private void serializeString(String string, OutputStream byteOS) throws IOException {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        serializeInt(bytes.length, byteOS);
        byteOS.write(bytes);
    }

    private void serializeYson(YTreeNode node, OutputStream byteOS) throws IOException {
        var byteOutputStreamForYson = new ByteArrayOutputStream();
        YTreeBinarySerializer.serialize(node, byteOutputStreamForYson);
        byte[] bytes = byteOutputStreamForYson.toByteArray();
        serializeInt(bytes.length, byteOS);
        byteOS.write(bytes);
    }

    @Nullable
    private <ObjectType> ObjectType deserializeObject(SkiffParser parser,
                                                                Class<ObjectType> clazz,
                                                                TiType tiType,
                                                                List<Type> genericTypeParameters) {
        tiType = extractSchemeFromOptional(parser, tiType);

        if (isSimpleType(tiType)) {
            return deserializeSimpleType(parser, tiType);
        }
        if (tiType.isDecimal()) {
            if (!clazz.equals(BigDecimal.class)) {
                throwInvalidSchemeException(null);
            }
            return castToType(deserializeDecimal(parser, tiType.asDecimal()));
        }

        return deserializeComplexObject(parser, clazz, tiType, genericTypeParameters);
    }

    private BigDecimal deserializeDecimal(SkiffParser parser,
                                          DecimalType decimalType) {
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

    private <ObjectType> ObjectType deserializeComplexObject(SkiffParser parser,
                                                             Class<ObjectType> clazz,
                                                             TiType tiType,
                                                             List<Type> genericTypeParameters) {
        if (List.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    tiType,
                    ArrayList.class
            ));
        }
        if (Set.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    tiType,
                    HashSet.class
            ));
        }
        if (Queue.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    tiType,
                    LinkedList.class
            ));
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return castToType(deserializeMap(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    genericTypeParameters.get(1),
                    tiType)
            );
        }
        if (clazz.isArray()) {
            return deserializeArray(parser, clazz, tiType);
        }

        return deserializeEntity(parser, clazz, tiType);
    }

    private <ObjectType> ObjectType deserializeEntity(SkiffParser parser,
                                                      Class<ObjectType> clazz,
                                                      TiType tiType) {
        if (!tiType.isStruct()) {
            throwInvalidSchemeException(null);
        }

        var defaultConstructor = complexObjectConstructorMap.computeIfAbsent(clazz, objectClass -> {
            try {
                var constructor = objectClass.getDeclaredConstructor();
                constructor.setAccessible(true);
                return constructor;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Entity must have empty constructor", e);
            }
        });

        ObjectType object = getInstanceWithoutArguments(defaultConstructor);

        var fieldDescriptions = entityFieldsMap.computeIfAbsent(clazz, entityClass -> {
            var declaredFields = getAllDeclaredFields(entityClass);
            setFieldsAccessibleToTrue(declaredFields);
            return EntityFieldDescr.of(declaredFields);
        });
        int indexInSchema = 0;
        for (var fieldDescr : fieldDescriptions) {
            if (fieldDescr.isTransient()) {
                continue;
            }
            try {
                fieldDescr.getField().set(
                        object,
                        deserializeObject(
                                parser,
                                castToType(fieldDescr.getField().getType()),
                                tiType.asStruct().getMembers().get(indexInSchema).getType(),
                                fieldDescr.getTypeParameters()
                        )
                );
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException e) {
                throwInvalidSchemeException(e);
            }
        }
        return object;
    }

    private <ElemType> Collection<ElemType> deserializeCollection(SkiffParser parser,
                                                                  Class<?> clazz,
                                                                  Type elementType,
                                                                  TiType tiType,
                                                                  Class<?> defaultClass) {
        if (!tiType.isList()) {
            throwInvalidSchemeException(null);
        }

        var collectionConstructor = complexObjectConstructorMap.computeIfAbsent(
                clazz,
                getConstructorOrDefaultFor(defaultClass)
        );

        Collection<ElemType> collection = getInstanceWithoutArguments(collectionConstructor);
        var elementTypeDescr = getTypeDescription(elementType);
        while (parser.parseInt8() != END_TAG) {
            collection.add(castToType(deserializeObject(
                            parser,
                            elementTypeDescr.getTypeClass(),
                            tiType.asList().getItem(),
                            elementTypeDescr.getTypeParameters()
                    ))
            );
        }
        return collection;
    }


    private <KeyType, ValueType> Map<KeyType, ValueType> deserializeMap(SkiffParser parser,
                                                                        Class<?> clazz,
                                                                        Type keyType,
                                                                        Type valueType,
                                                                        TiType tiType) {
        if (!tiType.isDict()) {
            throwInvalidSchemeException(null);
        }

        var mapConstructor = complexObjectConstructorMap.computeIfAbsent(
                clazz,
                getConstructorOrDefaultFor(HashMap.class)
        );

        Map<KeyType, ValueType> map = getInstanceWithoutArguments(mapConstructor);
        var keyTypeDescr = getTypeDescription(keyType);
        var valueTypeDescr = getTypeDescription(valueType);
        while (parser.parseInt8() != END_TAG) {
            map.put(castToType(deserializeObject(
                            parser,
                            keyTypeDescr.getTypeClass(),
                            tiType.asDict().getKey(),
                            keyTypeDescr.getTypeParameters()
                    )),
                    castToType(deserializeObject(
                            parser,
                            valueTypeDescr.getTypeClass(),
                            tiType.asDict().getValue(),
                            valueTypeDescr.getTypeParameters()
                    ))
            );
        }
        return map;
    }

    private <ArrayType, ElemType> ArrayType deserializeArray(SkiffParser parser,
                                                             Class<?> clazz,
                                                             TiType tiType) {
        var elementClass = clazz.getComponentType();
        List<ElemType> list = castToType(deserializeCollection(
                parser, clazz, elementClass, tiType, ArrayList.class
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

    private <SimpleType> SimpleType deserializeSimpleType(SkiffParser parser, TiType tiType) {
        try {
            if (tiType.isInt8()) {
                return castToType(parser.parseInt8());
            } else if (tiType.isInt16()) {
                return castToType(parser.parseInt16());
            } else if (tiType.isInt32()) {
                return castToType(parser.parseInt32());
            } else if (tiType.isInt64()) {
                return castToType(parser.parseInt64());
            } else if (tiType.isDouble()) {
                return castToType(parser.parseDouble());
            } else if (tiType.isBool()) {
                return castToType(parser.parseBoolean());
            } else if (tiType.isString()) {
                return deserializeString(parser);
            } else if (tiType.isYson()) {
                return deserializeYson(parser);
            } else if (tiType.isNull()) {
                return null;
            }
            throw new IllegalArgumentException(
                    String.format("Type '%s' is not supported", tiType));
        } catch (ClassCastException e) {
            throwInvalidSchemeException(e);
        }
        throw new IllegalStateException();
    }

    private <StringType> StringType deserializeString(SkiffParser parser) {
        BufferReference ref = parser.parseString32();
        return castToType(new String(ref.getBuffer(), ref.getOffset(),
                ref.getLength(), StandardCharsets.UTF_8));
    }

    private <YTreeNodeType> YTreeNodeType deserializeYson(SkiffParser parser) {
        BufferReference ref = parser.parseYson32();
        return castToType(YTreeBinarySerializer.deserialize(
                new ByteArrayInputStream(ref.getBuffer(), ref.getOffset(), ref.getLength())));
    }

    private TiType extractSchemeFromOptional(SkiffParser parser, TiType tiType) {
        if (!tiType.isOptional()) {
            return tiType;
        }
        return parser.parseVariant8Tag() == ZERO_TAG ?
                TiType.nullType() : tiType.asOptional().getItem();
    }

    private void throwInvalidSchemeException(@Nullable Exception e) {
        throw new IllegalStateException("Scheme does not correspond to object", e);
    }

    private void throwNoSchemaException() {
        throw new IllegalStateException("Cannot create or get table schema");
    }

    public static class MismatchEntityAndTableSchemaException extends RuntimeException {
        public MismatchEntityAndTableSchemaException(String message) {
            super(message);
        }
    }
}
