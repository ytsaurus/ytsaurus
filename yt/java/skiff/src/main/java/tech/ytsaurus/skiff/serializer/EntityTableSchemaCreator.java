package tech.ytsaurus.skiff.serializer;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Transient;

import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.type_info.StructType;
import tech.ytsaurus.type_info.TiType;

import static tech.ytsaurus.core.utils.ClassUtils.getAllDeclaredFields;
import static tech.ytsaurus.core.utils.ClassUtils.getTypeParameterOfGeneric;
import static tech.ytsaurus.core.utils.ClassUtils.isFieldTransient;
import static tech.ytsaurus.skiff.serializer.TiTypeUtil.getTiTypeIfSimple;

public class EntityTableSchemaCreator {

    private EntityTableSchemaCreator() {
    }

    public static <T> TableSchema create(Class<T> annotatedClass) {
        if (!annotatedClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        for (Field field : getAllDeclaredFields(annotatedClass)) {
            if (isFieldTransient(field, Transient.class)) {
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
        return getClassColumnSchema(field.getType(), field.getName(),
                !field.getType().isPrimitive(), field.getAnnotation(Column.class));
    }

    private static <T> ColumnSchema getClassColumnSchema(Class<T> clazz, String name,
                                                         boolean isNullable,
                                                         @Nullable Annotation annotation) {
        Optional<TiType> tiTypeIfSimple = getTiTypeIfSimple(clazz);
        if (annotation != null && Column.class.isAssignableFrom(annotation.getClass())) {
            Column column = (Column) annotation;
            name = column.name();
            isNullable = column.nullable();
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
            if (isFieldTransient(field, Transient.class)) {
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
        if (fieldWithCollection.isAnnotationPresent(Column.class)) {
            Column column = fieldWithCollection.getAnnotation(Column.class);
            name = column.name();
            isNullable = column.nullable();
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
