package tech.ytsaurus.client.rows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.math.BigDecimal;
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
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.skiff.SkiffParser;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.SkiffSerializer;
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

@NonNullApi
@NonNullFields
public class EntitySkiffSerializer<T> {
    private static final byte ZERO_TAG = 0x00;
    private static final byte ONE_TAG = 0x01;
    private static final byte END_TAG = (byte) 0xFF;
    private final Class<T> entityClass;
    private final List<EntityFieldDescr> entityFieldDescriptions;
    private final Map<Class<?>, List<EntityFieldDescr>> entityFieldsMap = new HashMap<>();
    private final Map<Class<?>, Constructor<?>> complexObjectConstructorMap = new HashMap<>();
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
        serializeEntity(object, entityTiType, entityFieldDescriptions, serializer);
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

        Class<?> clazz = object.getClass();
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

        serializeComplexObject(object, tiType, clazz, serializer);
    }

    private void serializeDecimal(
            BigDecimal value,
            DecimalType decimalType,
            SkiffSerializer serializer
    ) {
        byte[] dataInBigEndian = Decimal.textToBinary(
                value.toString(),
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
                serializer.serializeShort((short) object);
            } else if (tiType.isInt32()) {
                serializer.serializeInt((int) object);
            } else if (tiType.isInt64()) {
                serializer.serializeLong((long) object);
            } else if (tiType.isUint8()) {
                serializer.serializeUint8((Long) object);
            } else if (tiType.isUint16()) {
                serializer.serializeUint16((Long) object);
            } else if (tiType.isUint32()) {
                serializer.serializeUint32((Long) object);
            } else if (tiType.isUint64()) {
                serializer.serializeUint64((Long) object);
            } else if (tiType.isDouble()) {
                serializer.serializeDouble((double) object);
            } else if (tiType.isBool()) {
                serializer.serializeBoolean((boolean) object);
            } else if (tiType.isString()) {
                serializer.serializeString((String) object);
            } else if (tiType.isYson()) {
                serializer.serializeYson((YTreeNode) object);
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

    private <ObjectType> void serializeComplexObject(
            ObjectType object,
            TiType objectTiType,
            Class<?> clazz,
            SkiffSerializer serializer
    ) {
        if (Collection.class.isAssignableFrom(object.getClass())) {
            serializeCollection(object, objectTiType, serializer);
            return;
        }
        if (Map.class.isAssignableFrom(object.getClass())) {
            serializeMap(object, objectTiType, serializer);
            return;
        }
        if (clazz.isArray()) {
            serializeArray(object, objectTiType, serializer);
            return;
        }

        var entityFields = entityFieldsMap.computeIfAbsent(clazz, entityClass -> {
            var declaredFields = getAllDeclaredFields(entityClass);
            setFieldsAccessibleToTrue(declaredFields);
            return EntityFieldDescr.of(declaredFields);
        });
        serializeEntity(object, objectTiType, entityFields, serializer);
    }

    private <ObjectType> void serializeEntity(
            ObjectType object,
            TiType structTiType,
            List<EntityFieldDescr> fieldDescriptions,
            SkiffSerializer serializer
    ) {
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
                        serializer
                );
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException e) {
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

    private <ListType, KeyType, ValueType> void serializeMap(
            ListType object,
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
        tiType = extractSchemeFromOptional(tiType, parser);

        if (isSimpleType(tiType)) {
            return deserializeSimpleType(tiType, parser);
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
                                castToType(fieldDescr.getField().getType()),
                                tiType.asStruct().getMembers().get(indexInSchema).getType(),
                                fieldDescr.getTypeParameters(),
                                parser
                        )
                );
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException e) {
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

        var collectionConstructor = complexObjectConstructorMap.computeIfAbsent(
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

        var mapConstructor = complexObjectConstructorMap.computeIfAbsent(
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

    private <SimpleType> @Nullable SimpleType deserializeSimpleType(
            TiType tiType,
            SkiffParser parser
    ) {
        try {
            if (tiType.isInt8()) {
                return castToType(parser.parseInt8());
            } else if (tiType.isInt16()) {
                return castToType(parser.parseInt16());
            } else if (tiType.isInt32()) {
                return castToType(parser.parseInt32());
            } else if (tiType.isInt64()) {
                return castToType(parser.parseInt64());
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

    private TiType extractSchemeFromOptional(TiType tiType, SkiffParser parser) {
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
