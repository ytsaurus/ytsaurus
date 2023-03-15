package tech.ytsaurus.skiff.serialization;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import tech.ytsaurus.skiff.schema.ComplexSchema;
import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.skiff.schema.WireType;

import static tech.ytsaurus.core.utils.ClassUtils.anyMatchWithAnnotation;
import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getAnnotationIfPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeDescription;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfField;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;
import static tech.ytsaurus.skiff.schema.WireTypeUtil.getClassWireType;

public class EntitySkiffSchemaCreator {

    private EntitySkiffSchemaCreator() {
    }

    public static <T> SkiffSchema create(Class<T> annotatedClass) {
        if (!anyOfAnnotationsPresent(annotatedClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        return getComplexTypeSchema(annotatedClass);
    }

    private static <T> SkiffSchema getClassSchema(Class<T> clazz,
                                                  String name,
                                                  @Nullable Annotation annotation,
                                                  List<Type> genericTypeParameters) {
        WireType wireType = getClassWireType(clazz);
        boolean isNullable = true;
        if (annotation != null &&
                anyMatchWithAnnotation(annotation, JavaPersistenceApi.columnAnnotations())) {
            name = JavaPersistenceApi.getColumnName(annotation);
            isNullable = JavaPersistenceApi.isColumnNullable(annotation);
        }

        var schema = getSchemaByWireType(clazz, wireType, genericTypeParameters);

        if (isNullable && !clazz.isPrimitive()) {
            schema = SkiffSchema.variant8(
                    Arrays.asList(SkiffSchema.nothing(), schema));
        }
        if (!name.isEmpty()) {
            schema.setName(name);
        }

        return schema;
    }

    private static <T> SkiffSchema getSchemaByWireType(Class<T> clazz,
                                                       WireType wireType,
                                                       List<Type> genericTypeParameters) {
        if (Collection.class.isAssignableFrom(clazz)) {
            return getCollectionTypeSchema(genericTypeParameters.get(0));
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return getMapTypeSchema(genericTypeParameters.get(0), genericTypeParameters.get(1));
        }
        if (clazz.isArray()) {
            return getArraySchema(clazz);
        }
        return wireType.isSimpleType() ?
                SkiffSchema.simpleType(wireType) :
                getComplexTypeSchema(clazz);
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
        return getClassSchema(
                field.getType(),
                field.getName(),
                getAnnotationIfPresent(field, JavaPersistenceApi.columnAnnotations()).orElse(null),
                getTypeParametersOfField(field)
        );
    }

    private static ComplexSchema getCollectionTypeSchema(Type elementType) {
        var elementTypeDescr = getTypeDescription(elementType);
        return SkiffSchema.repeatedVariant8(List.of(
                getClassSchema(
                        elementTypeDescr.getTypeClass(),
                        "",
                        null,
                        elementTypeDescr.getTypeParameters()))
        );
    }

    private static ComplexSchema getMapTypeSchema(Type keyType, Type valueType) {
        var keyTypeDescr = getTypeDescription(keyType);
        var valueTypeDescr = getTypeDescription(valueType);
        return SkiffSchema.repeatedVariant8(List.of(
                        SkiffSchema.tuple(List.of(
                                getClassSchema(
                                        keyTypeDescr.getTypeClass(),
                                        "",
                                        null,
                                        keyTypeDescr.getTypeParameters()),
                                getClassSchema(
                                        valueTypeDescr.getTypeClass(),
                                        "",
                                        null,
                                        valueTypeDescr.getTypeParameters()))
                        )
                )
        );
    }

    private static ComplexSchema getArraySchema(Class<?> arrayClass) {
        if (!arrayClass.isArray()) {
            throw new IllegalArgumentException("Argument must be array");
        }
        return SkiffSchema.repeatedVariant8(List.of(
                getClassSchema(arrayClass.getComponentType(),
                        "",
                        null,
                        List.of()))
        );
    }
}
