package tech.ytsaurus.skiff.serialization;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;
import tech.ytsaurus.yson.BufferReference;

import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.castToList;
import static tech.ytsaurus.core.utils.ClassUtils.castToType;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParameterOfGeneric;
import static tech.ytsaurus.core.utils.ClassUtils.setFieldsAccessibleToTrue;
import static tech.ytsaurus.skiff.serialization.EntitySkiffSchemaCreator.getEntitySchema;

public class EntitySkiffDeserializer<T> {
    private final Class<T> entityClass;
    private final SkiffSchema schema;
    private final HashMap<Class<?>, List<EntityFieldDescr>> entityFieldsMap = new HashMap<>();

    public EntitySkiffDeserializer(Class<T> entityClass) {
        if (!anyOfAnnotationsPresent(entityClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }
        this.entityClass = entityClass;
        this.schema = getEntitySchema(entityClass);

        Field[] declaredFields = entityClass.getDeclaredFields();
        var entityFieldDescriptions = EntityFieldDescr.of(declaredFields);
        setFieldsAccessibleToTrue(declaredFields);
        entityFieldsMap.put(entityClass, entityFieldDescriptions);
    }

    public Optional<T> deserialize(byte[] objectBytes) {
        return deserialize(new SkiffParser(new ByteArrayInputStream(objectBytes)));
    }

    public Optional<T> deserialize(SkiffParser parser) {
        return Optional.ofNullable(deserializeObject(parser, entityClass, schema));
    }

    private <Type> @Nullable Type deserializeObject(SkiffParser parser, Class<Type> clazz, SkiffSchema schema) {
        schema = extractSchemeFromVariant(parser, schema);

        if (schema.getWireType().isSimpleType()) {
            return deserializeSimpleType(parser, schema);
        }

        return deserializeComplexObject(parser, clazz, schema);
    }

    private <Type> Type deserializeComplexObject(SkiffParser parser,
                                                 Class<Type> clazz,
                                                 SkiffSchema schema) {
        if (schema.getWireType() != WireType.TUPLE) {
            throwInvalidSchemeException(null);
        }

        Type object;
        try {
            Constructor<Type> defaultConstructor = clazz.getDeclaredConstructor();
            defaultConstructor.setAccessible(true);
            object = defaultConstructor.newInstance();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Entity must have empty constructor", e);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        var fieldDescriptions = entityFieldsMap.computeIfAbsent(clazz, entityClass -> {
            Field[] declaredFields = entityClass.getDeclaredFields();
            setFieldsAccessibleToTrue(declaredFields);
            return EntityFieldDescr.of(declaredFields);
        });
        int indexInSchema = 0;
        for (var fieldDescr : fieldDescriptions) {
            if (fieldDescr.isTransient()) {
                continue;
            }
            try {
                fieldDescr.getField().set(object, deserializeField(
                        parser, fieldDescr.getField(), schema.getChildren().get(indexInSchema)));
                indexInSchema++;
            } catch (IllegalAccessException | IndexOutOfBoundsException e) {
                throwInvalidSchemeException(e);
            }
        }
        return object;
    }

    private <Type> @Nullable Type deserializeField(SkiffParser parser, Field field, SkiffSchema schema) {
        if (Collection.class.isAssignableFrom(field.getType())) {
            schema = extractSchemeFromVariant(parser, schema);
            return deserializeCollection(parser, castToType(field.getType()), getTypeParameterOfGeneric(field), schema);
        }
        return deserializeObject(parser, castToType(field.getType()), schema);
    }

    private <Type, ElemType> Type deserializeCollection(SkiffParser parser,
                                                        Class<Type> clazz,
                                                        Class<ElemType> typeParameter,
                                                        SkiffSchema schema) {
        if (List.class.isAssignableFrom(clazz)) {
            return deserializeList(parser, typeParameter, schema);
        } else {
            throw new IllegalArgumentException("This collection (\"" + clazz.getName() +
                    "\") is not supported");
        }
    }

    private <Type, ElemType> Type deserializeList(SkiffParser parser,
                                                  Class<ElemType> typeParameter,
                                                  SkiffSchema schema) {
        if (!schema.isListSchema()) {
            throwInvalidSchemeException(null);
        }

        List<ElemType> list = castToList(new ArrayList<ElemType>());
        byte tag;
        while ((tag = parser.parseInt8()) != (byte) 0xFF) {
            list.add(deserializeObject(parser, typeParameter, schema.getChildren().get(tag)));
        }
        return castToType(list);
    }

    private static <Type> @Nullable Type deserializeSimpleType(SkiffParser parser, SkiffSchema schema) {
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
                    BufferReference ref = parser.parseString32();
                    return castToType(new String(ref.getBuffer(), ref.getOffset(),
                            ref.getLength(), StandardCharsets.UTF_8));
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

    private static void throwInvalidSchemeException(@Nullable Exception e) {
        throw new IllegalStateException("Scheme does not correspond to object", e);
    }
}
