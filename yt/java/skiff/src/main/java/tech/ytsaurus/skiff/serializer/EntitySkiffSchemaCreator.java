package tech.ytsaurus.skiff.serializer;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import tech.ytsaurus.skiff.schema.ComplexSchema;
import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;

import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParameterOfGeneric;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;
import static tech.ytsaurus.skiff.schema.WireTypeUtil.getClassWireType;

public class EntitySkiffSchemaCreator {

    private EntitySkiffSchemaCreator() {
    }

    public static <T> SkiffSchema getEntitySchema(Class<T> annotatedClass) {
        if (!annotatedClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        var entityAnnotation = annotatedClass.getAnnotation(Entity.class);
        return getClassSchema(annotatedClass, entityAnnotation.name(),
                false, entityAnnotation);
    }

    private static <T> SkiffSchema getClassSchema(Class<T> clazz, String name,
                                                  boolean isNullable,
                                                  @Nullable Annotation annotation) {
        WireType wireType = getClassWireType(clazz);
        if (annotation != null && Column.class.isAssignableFrom(annotation.getClass())) {
            Column column = (Column) annotation;
            name = column.name();
            isNullable = column.nullable();
        }

        SkiffSchema schema = wireType.isSimpleType() ?
                SkiffSchema.simpleType(wireType) :
                getComplexTypeSchema(clazz);

        if (isNullable && !clazz.isPrimitive()) {
            schema = SkiffSchema.variant8(
                    Arrays.asList(SkiffSchema.nothing(), schema));
        }
        if (!name.isEmpty()) {
            schema.setName(name);
        }

        return schema;
    }

    private static <T> ComplexSchema getComplexTypeSchema(Class<T> clazz) {
        ComplexSchema schema = SkiffSchema.tuple(new ArrayList<>());
        for (Field field : getAllDeclaredFields(clazz)) {
            if (isFieldTransient(field, Transient.class)) {
                continue;
            }
            schema.getChildren().add(getFieldSchema(field));
        }

        return schema;
    }

    private static SkiffSchema getFieldSchema(Field field) {
        if (Collection.class.isAssignableFrom(field.getType())) {
            return getCollectionFieldSchema(field);
        }
        return getClassSchema(field.getType(), field.getName(),
                !field.getType().isPrimitive(), field.getAnnotation(Column.class));
    }

    private static ComplexSchema getCollectionFieldSchema(Field fieldWithCollection) {
        Class<?> elementType = getTypeParameterOfGeneric(fieldWithCollection);
        ComplexSchema schema;
        if (List.class.isAssignableFrom(fieldWithCollection.getType())) {
            schema = getListFieldSchema(elementType);
        } else {
            throw new IllegalArgumentException("This collection (\"" + fieldWithCollection.getType().getName() +
                    "\") is not supported");
        }

        String name = fieldWithCollection.getName();
        boolean isNullable = true;
        if (fieldWithCollection.isAnnotationPresent(Column.class)) {
            Column column = fieldWithCollection.getAnnotation(Column.class);
            name = column.name();
            isNullable = column.nullable();
        }

        if (isNullable) {
            schema = SkiffSchema.variant8(
                    Arrays.asList(SkiffSchema.nothing(), schema));
        }
        schema.setName(name);

        return schema;
    }

    private static ComplexSchema getListFieldSchema(Class<?> elementType) {
        return SkiffSchema.repeatedVariant8(List.of(
                SkiffSchema.nothing(),
                getClassSchema(elementType, "", false, null))
        );
    }
}
