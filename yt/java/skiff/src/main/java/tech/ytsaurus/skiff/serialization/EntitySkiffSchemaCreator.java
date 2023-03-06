package tech.ytsaurus.skiff.serialization;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfGeneric;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfGenericField;
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

        SkiffSchema schema;
        if (Collection.class.isAssignableFrom(clazz)) {
            schema = getCollectionTypeSchema(clazz, genericTypeParameters);
        } else if (clazz.isArray()) {
            schema = getArraySchema(clazz);
        } else {
            schema = wireType.isSimpleType() ?
                    SkiffSchema.simpleType(wireType) :
                    getComplexTypeSchema(clazz);
        }

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
        List<Type> genericTypeParameters = new ArrayList<>();
        if (Collection.class.isAssignableFrom(field.getType())) {
            genericTypeParameters.addAll(getTypeParametersOfGenericField(field));
        }
        return getClassSchema(
                field.getType(),
                field.getName(),
                getAnnotationIfPresent(field, JavaPersistenceApi.columnAnnotations()).orElse(null),
                genericTypeParameters
        );
    }

    private static ComplexSchema getCollectionTypeSchema(Class<?> collectionClass, List<Type> typeParameters) {
        ComplexSchema schema;
        if (List.class.isAssignableFrom(collectionClass)) {
            schema = getListTypeSchema(typeParameters.get(0));
        } else {
            throw new IllegalArgumentException("This collection (\"" + collectionClass.getName() +
                    "\") is not supported");
        }
        return schema;
    }

    private static ComplexSchema getListTypeSchema(Type elementType) {
        Class<?> elementClass;
        List<Type> elementTypeParameters;
        if (elementType instanceof Class) {
            elementClass = (Class<?>) elementType;
            elementTypeParameters = List.of();
        } else if (elementType instanceof ParameterizedType) {
            elementClass = (Class<?>) ((ParameterizedType) elementType).getRawType();
            elementTypeParameters = getTypeParametersOfGeneric(elementType);
        } else {
            throw new IllegalArgumentException("Illegal list type parameter");
        }
        return SkiffSchema.repeatedVariant8(List.of(
                getClassSchema(
                        elementClass,
                        "",
                        null,
                        elementTypeParameters))
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
