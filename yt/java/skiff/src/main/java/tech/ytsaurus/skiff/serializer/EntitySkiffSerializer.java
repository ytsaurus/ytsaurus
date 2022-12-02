package tech.ytsaurus.skiff.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;
import javax.persistence.Entity;
import javax.persistence.Transient;

import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;

import static tech.ytsaurus.core.utils.ClassUtils.castToList;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;
import static tech.ytsaurus.skiff.schema.WireTypeUtil.getClassWireType;

public class EntitySkiffSerializer<T> {

    private final SkiffSchema schema;

    public EntitySkiffSerializer(SkiffSchema schema) {
        this.schema = schema;
    }

    public SkiffSchema getSchema() {
        return schema;
    }

    public byte[] serialize(T object) {
        if (!object.getClass().isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        serializeObject(object, schema, byteOS);
        try {
            byteOS.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOS.toByteArray();
    }

    private static <T> ByteArrayOutputStream serializeObject(@Nullable T object,
                                                             SkiffSchema objectSchema,
                                                             ByteArrayOutputStream byteOS) {
        boolean isNullable = objectSchema.getWireType().isVariant() &&
                objectSchema.getChildren().get(0).getWireType() == WireType.NOTHING;
        if (object == null) {
            if (!isNullable) {
                throw new NullPointerException("Field \"" + objectSchema.getName() + "\" is non nullable");
            }
            return serializeByte((byte) 0x00, byteOS);
        }
        if (isNullable) {
            serializeByte((byte) 0x01, byteOS);
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

        return serializeComplexObject(object, objectSchema, byteOS);
    }

    private static <T> ByteArrayOutputStream serializeComplexObject(T object,
                                                                    SkiffSchema objectSchema,
                                                                    ByteArrayOutputStream byteOS) {
        if (objectSchema.getWireType() != WireType.TUPLE) {
            throwInvalidSchemeException();
        }
        Field[] fields = object.getClass().getDeclaredFields();
        int indexInSchema = 0;
        for (Field field : fields) {
            if (isFieldTransient(field, Transient.class)) {
                continue;
            }
            try {
                field.setAccessible(true);
                serializeObject(
                        field.get(object),
                        objectSchema.getChildren().get(indexInSchema),
                        byteOS);
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException e) {
                throwInvalidSchemeException();
            }
        }
        return byteOS;
    }

    private static <T> ByteArrayOutputStream serializeCollection(T object,
                                                                 SkiffSchema collectionSchema,
                                                                 ByteArrayOutputStream byteOS) {
        if (List.class.isAssignableFrom(object.getClass())) {
            return serializeList(object, collectionSchema, byteOS);
        } else {
            throw new IllegalArgumentException("This collection (\"" + object.getClass().getName() +
                    "\") is not supported");
        }
    }

    private static <T, E> ByteArrayOutputStream serializeList(T object,
                                                              SkiffSchema listSchema,
                                                              ByteArrayOutputStream byteOS) {
        if (!listSchema.isListSchema()) {
            throwInvalidSchemeException();
        }

        List<E> list = castToList(object);
        for (E elem : list) {
            serializeObject(elem, listSchema, byteOS);
        }
        return serializeByte((byte) 0xFF, byteOS);
    }

    private static <T> ByteArrayOutputStream serializeSimpleType(T object,
                                                                 WireType wireType,
                                                                 ByteArrayOutputStream byteOS) {
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
        } catch (ClassCastException e) {
            throwInvalidSchemeException();
        }
        throw new IllegalStateException();
    }

    private static ByteArrayOutputStream serializeString(String string, ByteArrayOutputStream byteOS) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        serializeInt(bytes.length, byteOS)
                .writeBytes(bytes);
        return byteOS;
    }

    private static ByteArrayOutputStream serializeBoolean(boolean bool, ByteArrayOutputStream byteOS) {
        byteOS.writeBytes(bool ? new byte[]{1} : new byte[]{0});
        return byteOS;
    }

    private static ByteArrayOutputStream serializeDouble(double number, ByteArrayOutputStream byteOS) {
        byteOS.writeBytes(ByteBuffer
                .allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                .putDouble(number)
                .array());
        return byteOS;
    }

    private static ByteArrayOutputStream serializeByte(byte number, ByteArrayOutputStream byteOS) {
        byteOS.writeBytes(ByteBuffer
                .allocate(1).order(ByteOrder.LITTLE_ENDIAN)
                .put(number)
                .array());
        return byteOS;
    }

    private static ByteArrayOutputStream serializeShort(short number, ByteArrayOutputStream byteOS) {
        byteOS.writeBytes(ByteBuffer
                .allocate(2).order(ByteOrder.LITTLE_ENDIAN)
                .putShort(number)
                .array());
        return byteOS;
    }

    private static ByteArrayOutputStream serializeInt(int number, ByteArrayOutputStream byteOS) {
        byteOS.writeBytes(ByteBuffer
                .allocate(4).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(number)
                .array());
        return byteOS;
    }

    private static ByteArrayOutputStream serializeLong(long number, ByteArrayOutputStream byteOS) {
        byteOS.writeBytes(ByteBuffer
                .allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(number)
                .array());
        return byteOS;
    }


    private static void throwInvalidSchemeException() {
        throw new IllegalStateException("Scheme does not correspond to object");
    }
}
