package tech.ytsaurus.client.rows;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.StructType;
import tech.ytsaurus.typeinfo.TiType;

import static tech.ytsaurus.client.rows.TiTypeUtil.getTiTypeIfSimple;
import static tech.ytsaurus.core.utils.ClassUtils.anyMatchWithAnnotation;
import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getAnnotationIfPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeDescription;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParametersOfField;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;

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
        return getClassColumnSchema(
                field.getType(),
                field.getName(),
                getAnnotationIfPresent(field, JavaPersistenceApi.columnAnnotations()).orElse(null),
                getTypeParametersOfField(field)
        );
    }

    private static <T> ColumnSchema getClassColumnSchema(Class<T> clazz,
                                                         String name,
                                                         @Nullable Annotation annotation,
                                                         List<Type> genericTypeParameters) {
        boolean isNullable = true;
        if (annotation != null &&
                anyMatchWithAnnotation(annotation, JavaPersistenceApi.columnAnnotations())) {
            name = JavaPersistenceApi.getColumnName(annotation);
            isNullable = JavaPersistenceApi.isColumnNullable(annotation);
        }

        TiType tiType = getClassTiType(clazz, genericTypeParameters);

        if (isNullable && !clazz.isPrimitive()) {
            tiType = TiType.optional(tiType);
        }

        return new ColumnSchema(name, tiType);
    }

    private static <T> TiType getClassTiType(Class<T> clazz, List<Type> genericTypeParameters) {
        Optional<TiType> tiTypeIfSimple = getTiTypeIfSimple(clazz);
        if (Collection.class.isAssignableFrom(clazz)) {
            return getCollectionTiType(genericTypeParameters.get(0));
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return getMapTiType(genericTypeParameters.get(0), genericTypeParameters.get(1));
        }
        if (clazz.isArray()) {
            return getArrayTiType(clazz);
        }
        return tiTypeIfSimple.orElseGet(() -> getEntityTiType(clazz));
    }

    private static <T> TiType getEntityTiType(Class<T> clazz) {
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

    private static TiType getCollectionTiType(Type elementType) {
        var elementTypeDescr = getTypeDescription(elementType);
        return TiType.list(
                getClassColumnSchema(
                        elementTypeDescr.getTypeClass(),
                        "",
                        null,
                        elementTypeDescr.getTypeParameters())
                        .getTypeV3()
        );
    }

    private static TiType getMapTiType(Type keyType, Type valueType) {
        var keyTypeDescr = getTypeDescription(keyType);
        var valueTypeDescr = getTypeDescription(valueType);
        return TiType.dict(
                getClassColumnSchema(
                        keyTypeDescr.getTypeClass(),
                        "",
                        null,
                        keyTypeDescr.getTypeParameters())
                        .getTypeV3(),
                getClassColumnSchema(
                        valueTypeDescr.getTypeClass(),
                        "",
                        null,
                        valueTypeDescr.getTypeParameters())
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
