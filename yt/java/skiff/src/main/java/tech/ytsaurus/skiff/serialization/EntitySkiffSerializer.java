package tech.ytsaurus.skiff.serialization;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.annotation.Nullable;

import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;

import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.castToList;
import static tech.ytsaurus.core.utils.ClassUtils.setFieldsAccessibleToTrue;
import static tech.ytsaurus.skiff.schema.WireTypeUtil.getClassWireType;
import static tech.ytsaurus.skiff.serialization.EntitySkiffSchemaCreator.getEntitySchema;

public class EntitySkiffSerializer<T> {
    private final SkiffSchema schema;
    private final List<EntityFieldDescr> entityFieldDescriptions;
    private final HashMap<Class<?>, List<EntityFieldDescr>> entityFieldsMap = new HashMap<>();

    public EntitySkiffSerializer(Class<T> entityClass) {
        if (!anyOfAnnotationsPresent(entityClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }
        this.schema = getEntitySchema(entityClass);

        Field[] declaredFields = entityClass.getDeclaredFields();
        this.entityFieldDescriptions = EntityFieldDescr.of(declaredFields);
        setFieldsAccessibleToTrue(declaredFields);
        entityFieldsMap.put(entityClass, entityFieldDescriptions);
    }

    public SkiffSchema getSchema() {
        return schema;
    }

    public byte[] serialize(T object) {
        ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        try {
            serializeComplexObject(object, schema, entityFieldDescriptions, byteOS);
            byteOS.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOS.toByteArray();
    }

    public void serialize(T object, BufferedOutputStream outputStream) {
        serializeComplexObject(object, schema, entityFieldDescriptions, outputStream);
    }

    private <Type> OutputStream serializeObject(@Nullable Type object,
                                                SkiffSchema objectSchema,
                                                OutputStream byteOS) throws IOException {
        boolean isNullable = objectSchema.getWireType().isVariant() &&
                objectSchema.getChildren().get(0).getWireType() == WireType.NOTHING;
        if (object == null) {
            if (!isNullable) {
                throw new NullPointerException("Field \"" + objectSchema.getName() + "\" is non nullable");
            }
            byteOS.write(0x00);
            return byteOS;
        }
        if (isNullable) {
            byteOS.write(0x01);
            objectSchema = objectSchema.getChildren().get(1);
        }

        Class<?> clazz = object.getClass();
        WireType wireType = getClassWireType(clazz);
        if (wireType.isSimpleType()) {
            return serializeSimpleType(object, wireType, byteOS);
        }

        if (Collection.class.isAssignableFrom(clazz)) {
            return serializeCollection(object, objectSchema, byteOS);
        }

        var entityFields = entityFieldsMap.computeIfAbsent(clazz, entityClass -> {
            Field[] declaredFields = entityClass.getDeclaredFields();
            setFieldsAccessibleToTrue(declaredFields);
            return EntityFieldDescr.of(declaredFields);
        });
        return serializeComplexObject(object, objectSchema, entityFields, byteOS);
    }

    private <Type> OutputStream serializeComplexObject(Type object,
                                                       SkiffSchema objectSchema,
                                                       List<EntityFieldDescr> fieldDescriptions,
                                                       OutputStream byteOS) {
        if (objectSchema.getWireType() != WireType.TUPLE) {
            throwInvalidSchemeException();
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
                throwInvalidSchemeException();
            }
        }
        return byteOS;
    }

    private <Type> OutputStream serializeCollection(Type object,
                                                    SkiffSchema collectionSchema,
                                                    OutputStream byteOS) throws IOException {
        if (List.class.isAssignableFrom(object.getClass())) {
            return serializeList(object, collectionSchema, byteOS);
        } else {
            throw new IllegalArgumentException("This collection (\"" + object.getClass().getName() +
                    "\") is not supported");
        }
    }

    private <Type, ElemType> OutputStream serializeList(Type object,
                                                        SkiffSchema listSchema,
                                                        OutputStream byteOS) throws IOException {
        if (!listSchema.isListSchema()) {
            throwInvalidSchemeException();
        }

        List<ElemType> list = castToList(object);
        for (ElemType elem : list) {
            serializeObject(elem, listSchema, byteOS);
        }
        return serializeByte((byte) 0xFF, byteOS);
    }

    private static <Type> OutputStream serializeSimpleType(Type object,
                                                           WireType wireType,
                                                           OutputStream byteOS) {
        try {
            switch (wireType) {
                case INT_8:
                    return serializeByte((byte) object, byteOS);
                case INT_16:
                    return serializeShort((short) object, byteOS);
                case INT_32:
                    return serializeInt((int) object, byteOS);
                case INT_64:
                    return serializeLong((long) object, byteOS);
                case DOUBLE:
                    return serializeDouble((double) object, byteOS);
                case BOOLEAN:
                    return serializeBoolean((boolean) object, byteOS);
                case STRING_32:
                    return serializeString((String) object, byteOS);
                default:
                    throw new IllegalArgumentException("This type + (\"" + wireType + "\") is not supported");
            }
        } catch (ClassCastException | IOException e) {
            throwInvalidSchemeException();
        }
        throw new IllegalStateException();
    }

    private static OutputStream serializeString(String string, OutputStream byteOS) throws IOException {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        serializeInt(bytes.length, byteOS)
                .write(bytes);
        return byteOS;
    }

    private static OutputStream serializeBoolean(boolean bool, OutputStream byteOS) throws IOException {
        byteOS.write(bool ? 1 : 0);
        return byteOS;
    }

    private static OutputStream serializeDouble(double number, OutputStream byteOS) throws IOException {
        byteOS.write(ByteBuffer
                .allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                .putDouble(number)
                .array());
        return byteOS;
    }

    private static OutputStream serializeByte(byte number, OutputStream byteOS) throws IOException {
        byteOS.write(number);
        return byteOS;
    }

    private static OutputStream serializeShort(short number, OutputStream byteOS) throws IOException {
        byteOS.write((number & 0xFF));
        byteOS.write((number >> 8) & 0xFF);
        return byteOS;
    }

    private static OutputStream serializeInt(int number, OutputStream byteOS) throws IOException {
        byteOS.write((number & 0xFF));
        byteOS.write((number >> 8) & 0xFF);
        byteOS.write((number >> 16) & 0xFF);
        byteOS.write((number >> 24) & 0xFF);
        return byteOS;
    }

    private static OutputStream serializeLong(long number, OutputStream byteOS) throws IOException {
        byteOS.write((int) (number & 0xFF));
        byteOS.write((int) ((number >> 8) & 0xFF));
        byteOS.write((int) ((number >> 16) & 0xFF));
        byteOS.write((int) ((number >> 24) & 0xFF));
        byteOS.write((int) ((number >> 32) & 0xFF));
        byteOS.write((int) ((number >> 40) & 0xFF));
        byteOS.write((int) ((number >> 48) & 0xFF));
        byteOS.write((int) ((number >> 56) & 0xFF));
        return byteOS;
    }


    private static void throwInvalidSchemeException() {
        throw new IllegalStateException("Scheme does not correspond to object");
    }
}
