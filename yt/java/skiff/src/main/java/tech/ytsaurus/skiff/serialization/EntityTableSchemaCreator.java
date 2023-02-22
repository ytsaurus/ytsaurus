package tech.ytsaurus.skiff.serialization;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.type_info.StructType;
import tech.ytsaurus.type_info.TiType;

import static tech.ytsaurus.core.utils.ClassUtils.anyMatchWithAnnotation;
import static tech.ytsaurus.core.utils.ClassUtils.anyOfAnnotationsPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getAnnotationIfPresent;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParameterOfGeneric;
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
        if (Collection.class.isAssignableFrom(field.getType())) {
            return getCollectionColumnSchema(field);
        }
        return getClassColumnSchema(
                field.getType(),
                field.getName(),
                !field.getType().isPrimitive(),
                getAnnotationIfPresent(field, JavaPersistenceApi.columnAnnotations()).orElse(null)
        );
    }

    private static <T> ColumnSchema getClassColumnSchema(Class<T> clazz, String name,
                                                         boolean isNullable,
                                                         @Nullable Annotation annotation) {
        Optional<TiType> tiTypeIfSimple = getTiTypeIfSimple(clazz);
        if (annotation != null &&
                anyMatchWithAnnotation(annotation, JavaPersistenceApi.columnAnnotations())) {
            name = JavaPersistenceApi.getColumnName(annotation);
            isNullable = JavaPersistenceApi.isColumnNullable(annotation);
        }

        TiType tiType = tiTypeIfSimple.orElseGet(() -> getComplexTiType(clazz));
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

    private static ColumnSchema getCollectionColumnSchema(Field fieldWithCollection) {
        Class<?> elementType = getTypeParameterOfGeneric(fieldWithCollection);
        TiType tiType;
        if (List.class.isAssignableFrom(fieldWithCollection.getType())) {
            tiType = getListFieldTiType(elementType);
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
            tiType = TiType.optional(tiType);
        }

        return new ColumnSchema(name, tiType);
    }

    private static TiType getListFieldTiType(Class<?> elementType) {
        return getClassColumnSchema(elementType, "", false, null).getTypeV3();
    }
}
