package tech.ytsaurus.client.rows;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
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

import tech.ytsaurus.skiff.SkiffParser;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.skiff.WireType;
import tech.ytsaurus.yson.BufferReference;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.boxArray;
import static tech.ytsaurus.core.utils.ClassUtils.castToType;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getConstructorOrDefaultFor;
import static tech.ytsaurus.core.utils.ClassUtils.getInstanceWithoutArguments;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeDescription;
import static tech.ytsaurus.core.utils.ClassUtils.setFieldsAccessibleToTrue;
import static tech.ytsaurus.core.utils.ClassUtils.unboxArray;
import static tech.ytsaurus.skiff.WireTypeUtil.getClassWireType;

public class EntitySkiffSerializer<T> {
    private static final byte ZERO_TAG = 0x00;
    private static final byte ONE_TAG = 0x01;
    private static final byte END_TAG = (byte) 0xFF;
    private final Class<T> entityClass;
    private final SkiffSchema schema;
    private final List<EntityFieldDescr> entityFieldDescriptions;
    private final Map<Class<?>, List<EntityFieldDescr>> entityFieldsMap = new HashMap<>();
    private final Map<Class<?>, Constructor<?>> complexObjectConstructorMap = new HashMap<>();

    public EntitySkiffSerializer(Class<T> entityClass) {
        if (!anyOfAnnotationsPresent(entityClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }
        this.entityClass = entityClass;
        this.schema = EntitySkiffSchemaCreator.create(entityClass);

        var declaredFields = getAllDeclaredFields(entityClass);
        this.entityFieldDescriptions = EntityFieldDescr.of(declaredFields);
        setFieldsAccessibleToTrue(declaredFields);
        entityFieldsMap.put(entityClass, entityFieldDescriptions);
    }

    public SkiffSchema getSchema() {
        return schema;
    }

    public byte[] serialize(T object) {
        ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        serializeEntity(object, schema, entityFieldDescriptions, byteOS);
        try {
            byteOS.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOS.toByteArray();
    }

    public void serialize(T object, BufferedOutputStream outputStream) {
        serializeEntity(object, schema, entityFieldDescriptions, outputStream);
    }

    public Optional<T> deserialize(byte[] objectBytes) {
        return deserialize(new SkiffParser(new ByteArrayInputStream(objectBytes)));
    }

    public Optional<T> deserialize(SkiffParser parser) {
        return Optional.ofNullable(deserializeObject(parser, entityClass, schema, List.of()));
    }

    private <ObjectType> void serializeObject(@Nullable ObjectType object,
                                              SkiffSchema objectSchema,
                                              OutputStream byteOS) throws IOException {
        boolean isNullable = objectSchema.getWireType().isVariant() &&
                objectSchema.getChildren().get(0).getWireType() == WireType.NOTHING;
        if (object == null) {
            if (!isNullable) {
                throw new NullPointerException("Field \"" + objectSchema.getName() + "\" is non nullable");
            }
            byteOS.write(ZERO_TAG);
            return;
        }
        if (isNullable) {
            byteOS.write(ONE_TAG);
            objectSchema = objectSchema.getChildren().get(1);
        }

        Class<?> clazz = object.getClass();
        WireType wireType = getClassWireType(clazz);
        if (wireType.isSimpleType()) {
            serializeSimpleType(object, wireType, byteOS);
            return;
        }

        serializeComplexObject(object, objectSchema, byteOS, clazz);
    }

    private <ObjectType> void serializeComplexObject(ObjectType object,
                                                     SkiffSchema objectSchema,
                                                     OutputStream byteOS, Class<?> clazz) throws IOException {
        if (Collection.class.isAssignableFrom(object.getClass())) {
            serializeCollection(object, objectSchema, byteOS);
            return;
        }
        if (Map.class.isAssignableFrom(object.getClass())) {
            serializeMap(object, objectSchema, byteOS);
            return;
        }
        if (clazz.isArray()) {
            serializeArray(object, objectSchema, byteOS);
            return;
        }

        var entityFields = entityFieldsMap.computeIfAbsent(clazz, entityClass -> {
            var declaredFields = getAllDeclaredFields(entityClass);
            setFieldsAccessibleToTrue(declaredFields);
            return EntityFieldDescr.of(declaredFields);
        });
        serializeEntity(object, objectSchema, entityFields, byteOS);
    }

    private <ObjectType> void serializeEntity(ObjectType object,
                                              SkiffSchema objectSchema,
                                              List<EntityFieldDescr> fieldDescriptions,
                                              OutputStream byteOS) {
        if (objectSchema.getWireType() != WireType.TUPLE) {
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
                        objectSchema.getChildren().get(indexInSchema),
                        byteOS);
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException | IOException e) {
                throwInvalidSchemeException(e);
            }
        }
    }

    private <CollectionType, ElemType> void serializeCollection(CollectionType object,
                                                                SkiffSchema schema,
                                                                OutputStream byteOS) throws IOException {
        if (!schema.isListSchema()) {
            throwInvalidSchemeException(null);
        }

        Collection<ElemType> collection = castToType(object);
        SkiffSchema elemSchema = schema.getChildren().get(0);
        for (var elem : collection) {
            byteOS.write(ZERO_TAG);
            serializeObject(elem, elemSchema, byteOS);
        }
        serializeByte(END_TAG, byteOS);
    }

    private <ListType, KeyType, ValueType> void serializeMap(ListType object,
                                                             SkiffSchema mapSchema,
                                                             OutputStream byteOS) throws IOException {
        if (!mapSchema.isMapSchema()) {
            throwInvalidSchemeException(null);
        }

        Map<KeyType, ValueType> map = castToType(object);
        SkiffSchema keySchema = mapSchema.getChildren().get(0).getChildren().get(0);
        SkiffSchema valueSchema = mapSchema.getChildren().get(0).getChildren().get(1);
        for (var entry : map.entrySet()) {
            byteOS.write(ZERO_TAG);
            serializeObject(entry.getKey(), keySchema, byteOS);
            serializeObject(entry.getValue(), valueSchema, byteOS);
        }
        serializeByte(END_TAG, byteOS);
    }

    private <ArrayType, ElemType> void serializeArray(ArrayType object,
                                                      SkiffSchema listSchema,
                                                      OutputStream byteOS) throws IOException {
        if (!listSchema.isListSchema()) {
            throwInvalidSchemeException(null);
        }

        Class<?> elementClass = object.getClass().getComponentType();
        ElemType[] list = elementClass.isPrimitive() ?
                boxArray(object, elementClass) :
                castToType(object);
        SkiffSchema elemSchema = listSchema.getChildren().get(0);
        for (var elem : list) {
            byteOS.write(ZERO_TAG);
            serializeObject(elem, elemSchema, byteOS);
        }
        serializeByte(END_TAG, byteOS);
    }

    private <SimpleType> void serializeSimpleType(SimpleType object,
                                                  WireType wireType,
                                                  OutputStream byteOS) {
        try {
            switch (wireType) {
                case INT_8:
                    serializeByte((byte) object, byteOS);
                    return;
                case INT_16:
                    serializeShort((short) object, byteOS);
                    return;
                case INT_32:
                    serializeInt((int) object, byteOS);
                    return;
                case INT_64:
                    serializeLong((long) object, byteOS);
                    return;
                case DOUBLE:
                    serializeDouble((double) object, byteOS);
                    return;
                case BOOLEAN:
                    serializeBoolean((boolean) object, byteOS);
                    return;
                case STRING_32:
                    serializeString((String) object, byteOS);
                    return;
                case YSON_32:
                    serializeYson((YTreeNode) object, byteOS);
                    return;
                default:
                    throw new IllegalArgumentException("This type + (\"" + wireType + "\") is not supported");
            }
        } catch (ClassCastException | IOException e) {
            throwInvalidSchemeException(e);
        }
        throw new IllegalStateException();
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

    private <ObjectType> @Nullable ObjectType deserializeObject(SkiffParser parser,
                                                                Class<ObjectType> clazz,
                                                                SkiffSchema schema,
                                                                List<Type> genericTypeParameters) {
        schema = extractSchemeFromVariant(parser, schema);

        if (schema.getWireType().isSimpleType()) {
            return deserializeSimpleType(parser, schema);
        }

        return deserializeComplexObject(parser, clazz, schema, genericTypeParameters);
    }

    private <ObjectType> ObjectType deserializeComplexObject(SkiffParser parser,
                                                             Class<ObjectType> clazz,
                                                             SkiffSchema schema,
                                                             List<Type> genericTypeParameters) {
        if (List.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    schema,
                    ArrayList.class
            ));
        }
        if (Set.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    schema,
                    HashSet.class
            ));
        }
        if (Queue.class.isAssignableFrom(clazz)) {
            return castToType(deserializeCollection(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    schema,
                    LinkedList.class
            ));
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return castToType(deserializeMap(
                    parser,
                    clazz,
                    genericTypeParameters.get(0),
                    genericTypeParameters.get(1),
                    schema)
            );
        }
        if (clazz.isArray()) {
            return deserializeArray(parser, clazz, schema);
        }

        return deserializeEntity(parser, clazz, schema);
    }

    private <ObjectType> ObjectType deserializeEntity(SkiffParser parser,
                                                      Class<ObjectType> clazz,
                                                      SkiffSchema schema) {
        if (schema.getWireType() != WireType.TUPLE) {
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
                                extractSchemeFromVariant(parser, schema.getChildren().get(indexInSchema)),
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
                                                                  SkiffSchema schema,
                                                                  Class<?> defaultClass) {
        if (!schema.isListSchema()) {
            throwInvalidSchemeException(null);
        }

        var collectionConstructor = complexObjectConstructorMap.computeIfAbsent(
                clazz,
                getConstructorOrDefaultFor(defaultClass)
        );

        Collection<ElemType> collection = getInstanceWithoutArguments(collectionConstructor);
        var elementTypeDescr = getTypeDescription(elementType);
        byte tag;
        while ((tag = parser.parseInt8()) != END_TAG) {
            collection.add(castToType(deserializeObject(
                    parser,
                    elementTypeDescr.getTypeClass(),
                    schema.getChildren().get(tag),
                    elementTypeDescr.getTypeParameters()))
            );
        }
        return collection;
    }


    private <KeyType, ValueType> Map<KeyType, ValueType> deserializeMap(SkiffParser parser,
                                                                        Class<?> clazz,
                                                                        Type keyType,
                                                                        Type valueType,
                                                                        SkiffSchema schema) {
        if (!schema.isMapSchema()) {
            throwInvalidSchemeException(null);
        }

        var mapConstructor = complexObjectConstructorMap.computeIfAbsent(
                clazz,
                getConstructorOrDefaultFor(HashMap.class)
        );

        Map<KeyType, ValueType> map = getInstanceWithoutArguments(mapConstructor);
        var keyTypeDescr = getTypeDescription(keyType);
        var valueTypeDescr = getTypeDescription(valueType);
        byte tag;
        while ((tag = parser.parseInt8()) != END_TAG) {
            map.put(castToType(deserializeObject(
                            parser,
                            keyTypeDescr.getTypeClass(),
                            schema.getChildren().get(tag).getChildren().get(0),
                            keyTypeDescr.getTypeParameters())),
                    castToType(deserializeObject(
                            parser,
                            valueTypeDescr.getTypeClass(),
                            schema.getChildren().get(tag).getChildren().get(1),
                            valueTypeDescr.getTypeParameters()))
            );
        }
        return map;
    }

    private <ArrayType, ElemType> ArrayType deserializeArray(SkiffParser parser,
                                                             Class<?> clazz,
                                                             SkiffSchema schema) {
        var elementClass = clazz.getComponentType();
        List<ElemType> list = castToType(deserializeCollection(
                parser, clazz, elementClass, schema, ArrayList.class
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

    private <SimpleType> @Nullable SimpleType deserializeSimpleType(SkiffParser parser, SkiffSchema schema) {
        try {
            switch (schema.getWireType()) {
                case INT_8:
                    return castToType(parser.parseInt8());
                case INT_16:
                    return castToType(parser.parseInt16());
                case INT_32:
                    return castToType(parser.parseInt32());
                case INT_64:
                    return castToType(parser.parseInt64());
                case DOUBLE:
                    return castToType(parser.parseDouble());
                case BOOLEAN:
                    return castToType(parser.parseBoolean());
                case STRING_32:
                    return deserializeString(parser);
                case YSON_32:
                    return deserializeYson(parser);
                case NOTHING:
                    return null;
                default:
                    throw new IllegalArgumentException("This type + (\"" + schema.getWireType() +
                            "\") is not supported");
            }
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

    private SkiffSchema extractSchemeFromVariant(SkiffParser parser, SkiffSchema schema) {
        if (!schema.getWireType().isVariant()) {
            return schema;
        }

        int tag;
        if (schema.getWireType() == WireType.VARIANT_8) {
            tag = parser.parseVariant8Tag();
        } else {
            tag = parser.parseVariant16Tag();
        }

        return schema.getChildren().get(tag);
    }

    private void throwInvalidSchemeException(@Nullable Exception e) {
        throw new IllegalStateException("Scheme does not correspond to object", e);
    }
}
