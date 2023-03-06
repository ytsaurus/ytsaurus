package tech.ytsaurus.skiff.serialization;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.StructType;
import tech.ytsaurus.typeinfo.TiType;

import static tech.ytsaurus.core.utils.ClassUtils.anyMatchWithAnnotation;
import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getAnnotationIfPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfGeneric;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfGenericField;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;
import static tech.ytsaurus.skiff.serialization.TiTypeUtil.getTiTypeIfSimple;

public class EntityTableSchemaCreator {

    private EntityTableSchemaCreator() {
    }

    public static <T> TableSchema create(Class<T> annotatedClass) {
        if (!anyOfAnnotationsPresent(annotatedClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        for (Field field : getAllDeclaredFields(annotatedClass)) {
            if (isFieldTransient(field, JavaPersistenceApi.transientAnnotations())) {
                continue;
            }
            tableSchemaBuilder.add(getFieldColumnSchema(field));
        }

        return tableSchemaBuilder.build();
    }

    private static ColumnSchema getFieldColumnSchema(Field field) {
        List<Type> genericTypeParameters = new ArrayList<>();
        if (Collection.class.isAssignableFrom(field.getType())) {
            genericTypeParameters.addAll(getTypeParametersOfGenericField(field));
        }
        return getClassColumnSchema(
                field.getType(),
                field.getName(),
                getAnnotationIfPresent(field, JavaPersistenceApi.columnAnnotations()).orElse(null),
                genericTypeParameters
        );
    }

    private static <T> ColumnSchema getClassColumnSchema(Class<T> clazz,
                                                         String name,
                                                         @Nullable Annotation annotation,
                                                         List<Type> genericTypeParameters) {
        Optional<TiType> tiTypeIfSimple = getTiTypeIfSimple(clazz);
        boolean isNullable = true;
        if (annotation != null &&
                anyMatchWithAnnotation(annotation, JavaPersistenceApi.columnAnnotations())) {
            name = JavaPersistenceApi.getColumnName(annotation);
            isNullable = JavaPersistenceApi.isColumnNullable(annotation);
        }

        TiType tiType;
        if (Collection.class.isAssignableFrom(clazz)) {
            tiType = getCollectionTiType(clazz, genericTypeParameters);
        } else if (clazz.isArray()) {
            tiType = getArrayTiType(clazz);
        } else {
            tiType = tiTypeIfSimple.orElseGet(() -> getComplexTiType(clazz));
        }

        if (isNullable && !clazz.isPrimitive()) {
            tiType = TiType.optional(tiType);
        }

        return new ColumnSchema(name, tiType);
    }

    private static <T> TiType getComplexTiType(Class<T> clazz) {
        ArrayList<StructType.Member> members = new ArrayList<>();
        for (Field field : getAllDeclaredFields(clazz)) {
            if (isFieldTransient(field, JavaPersistenceApi.transientAnnotations())) {
                continue;
            }
            members.add(getStructMember(field));
        }

        return TiType.struct(members);
    }

    private static StructType.Member getStructMember(Field field) {
        var columnSchema = getFieldColumnSchema(field);
        return new StructType.Member(columnSchema.getName(), columnSchema.getTypeV3());
    }

    private static TiType getCollectionTiType(Class<?> collectionClass, List<Type> typeParameters) {
        TiType tiType;
        if (List.class.isAssignableFrom(collectionClass)) {
            tiType = getListTiType(typeParameters.get(0));
        } else {
            throw new IllegalArgumentException("This collection (\"" + collectionClass.getName() +
                    "\") is not supported");
        }
        return tiType;
    }

    private static TiType getListTiType(Type elementType) {
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
        return TiType.list(
                getClassColumnSchema(
                        elementClass,
                        "",
                        null,
                        elementTypeParameters)
                        .getTypeV3()
        );
    }

    private static TiType getArrayTiType(Class<?> arrayClass) {
        if (!arrayClass.isArray()) {
            throw new IllegalArgumentException("Argument must be array");
        }
        return TiType.list(
                getClassColumnSchema(
                        arrayClass.getComponentType(),
                        "",
                        null,
                        List.of())
                        .getTypeV3()
        );
    }
}
