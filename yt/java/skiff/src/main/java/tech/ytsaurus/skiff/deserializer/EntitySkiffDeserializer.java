package tech.ytsaurus.skiff.deserializer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.persistence.Entity;
import javax.persistence.Transient;

import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;

import static tech.ytsaurus.core.utils.ClassUtils.castToList;
import static tech.ytsaurus.core.utils.ClassUtils.castToType;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParameterOfGeneric;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;

public class EntitySkiffDeserializer {

    private EntitySkiffDeserializer() {
    }

    public static <T> Optional<T> deserialize(byte[] objectBytes,
                                              Class<T> objectClass,
                                              SkiffSchema schema) {
        if (!objectClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        return Optional.ofNullable(castToType(
                deserializeObject(new SkiffParser(objectBytes), objectClass, schema)));
    }

    private static <T> @Nullable T deserializeObject(SkiffParser parser, Class<T> clazz, SkiffSchema schema) {
        schema = extractSchemeFromVariant(parser, schema);

        if (schema.getWireType().isSimpleType()) {
            return deserializeSimpleType(parser, schema);
        }

        return deserializeComplexObject(parser, clazz, schema);
    }

    private static <T> T deserializeComplexObject(SkiffParser parser,
                                                  Class<T> clazz,
                                                  SkiffSchema schema) {
        if (schema.getWireType() != WireType.TUPLE) {
            throwInvalidSchemeException();
        }

        T object;
        try {
            Constructor<T> defaultConstructor = clazz.getDeclaredConstructor();
            defaultConstructor.setAccessible(true);
            object = defaultConstructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        Field[] fields = clazz.getDeclaredFields();
        int indexInSchema = 0;
        for (Field field : fields) {
            if (isFieldTransient(field, Transient.class)) {
                continue;
            }
            try {
                field.setAccessible(true);
                field.set(object, deserializeField(
                        parser, field, schema.getChildren().get(indexInSchema)));
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException e) {
                throwInvalidSchemeException();
            }
        }
        return object;
    }

    private static <T> @Nullable T deserializeField(SkiffParser parser, Field field, SkiffSchema schema) {
        if (Collection.class.isAssignableFrom(field.getType())) {
            schema = extractSchemeFromVariant(parser, schema);
            return deserializeCollection(parser, castToType(field.getType()), getTypeParameterOfGeneric(field), schema);
        }
        return deserializeObject(parser, castToType(field.getType()), schema);
    }

    private static <T, E> T deserializeCollection(SkiffParser parser,
                                                  Class<T> clazz,
                                                  Class<E> typeParameter,
                                                  SkiffSchema schema) {
        if (List.class.isAssignableFrom(clazz)) {
            return deserializeList(parser, typeParameter, schema);
        } else {
            throw new IllegalArgumentException("This collection (\"" + clazz.getName() +
                    "\") is not supported");
        }
    }

    private static <T, E> T deserializeList(SkiffParser parser,
                                            Class<E> typeParameter,
                                            SkiffSchema schema) {
        if (!schema.isListSchema()) {
            throwInvalidSchemeException();
        }

        List<E> list = castToList(new ArrayList<E>());
        byte tag;
        while ((tag = parser.parseInt8()) != (byte) 0xFF) {
            list.add(deserializeObject(parser, typeParameter, schema.getChildren().get(tag)));
        }
        return castToType(list);
    }

    private static <T> @Nullable T deserializeSimpleType(SkiffParser parser, SkiffSchema schema) {
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
                    return castToType(new String(parser.parseString32(), StandardCharsets.UTF_8));
                case NOTHING:
                    return null;
                default:
                    throw new IllegalArgumentException("This type + (\"" + schema.getWireType() +
                            "\") is not supported");
            }
        } catch (ClassCastException e) {
            throwInvalidSchemeException();
        }
        throw new IllegalStateException();
    }

    private static SkiffSchema extractSchemeFromVariant(SkiffParser parser, SkiffSchema schema) {
        if (!schema.getWireType().isVariant()) {
            return schema;
        }

        int tag;
        if (schema.getWireType() == WireType.VARIANT_8 ||
                schema.getWireType() == WireType.REPEATED_VARIANT_8) {
            tag = parser.parseVariant8Tag();
        } else {
            tag = parser.parseVariant16Tag();
        }

        return schema.getChildren().get(tag);
    }

    private static void throwInvalidSchemeException() {
        throw new IllegalStateException("Scheme does not correspond to object");
    }
}
