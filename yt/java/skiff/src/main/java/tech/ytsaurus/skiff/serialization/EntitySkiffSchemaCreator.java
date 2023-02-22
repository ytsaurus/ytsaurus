package tech.ytsaurus.skiff.serialization;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import tech.ytsaurus.skiff.schema.ComplexSchema;
import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;

import static tech.ytsaurus.core.utils.ClassUtils.anyMatchWithAnnotation;
import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getAnnotationIfPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParameterOfGeneric;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;
import static tech.ytsaurus.skiff.schema.WireTypeUtil.getClassWireType;

public class EntitySkiffSchemaCreator {

    private EntitySkiffSchemaCreator() {
    }

    public static <T> SkiffSchema getEntitySchema(Class<T> annotatedClass) {
        if (!anyOfAnnotationsPresent(annotatedClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        Annotation entityAnnotation = getAnnotationIfPresent(annotatedClass, JavaPersistenceApi.entityAnnotations())
                .orElseThrow(IllegalStateException::new);
        return getClassSchema(annotatedClass, JavaPersistenceApi.getEntityName(entityAnnotation),
                false, entityAnnotation);
    }

    private static <T> SkiffSchema getClassSchema(Class<T> clazz, String name,
                                                  boolean isNullable,
                                                  @Nullable Annotation annotation) {
        WireType wireType = getClassWireType(clazz);
        if (annotation != null &&
                anyMatchWithAnnotation(annotation, JavaPersistenceApi.columnAnnotations())) {
            name = JavaPersistenceApi.getColumnName(annotation);
            isNullable = JavaPersistenceApi.isColumnNullable(annotation);
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
            if (isFieldTransient(field, JavaPersistenceApi.transientAnnotations())) {
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
        return getClassSchema(
                field.getType(),
                field.getName(),
                !field.getType().isPrimitive(),
                getAnnotationIfPresent(field, JavaPersistenceApi.columnAnnotations()).orElse(null)
        );
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
        if (anyOfAnnotationsPresent(fieldWithCollection, JavaPersistenceApi.columnAnnotations())) {
            Annotation columnAnnotation =
                    getAnnotationIfPresent(fieldWithCollection, JavaPersistenceApi.columnAnnotations())
                            .orElseThrow(IllegalStateException::new);
            name = JavaPersistenceApi.getColumnName(columnAnnotation);
            isNullable = JavaPersistenceApi.isColumnNullable(columnAnnotation);
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
