package tech.ytsaurus.client.rows;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.DecimalType;
import tech.ytsaurus.typeinfo.DictType;
import tech.ytsaurus.typeinfo.ListType;
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

    public static <T> TableSchema create(Class<T> annotatedClass, @Nullable TableSchema schema) {
        if (!anyOfAnnotationsPresent(annotatedClass, JavaPersistenceApi.entityAnnotations())) {
            throw new IllegalArgumentException("Class must be annotated with @Entity");
        }

        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        StructType tableSchemaAsStructType = schema != null ?
                TiTypeUtil.tableSchemaToStructTiType(schema).asStruct() : null;
        for (Field field : getAllDeclaredFields(annotatedClass)) {
            if (isFieldTransient(field, JavaPersistenceApi.transientAnnotations())) {
                continue;
            }
            tableSchemaBuilder.add(
                    getFieldColumnSchema(field, tableSchemaAsStructType)
            );
        }

        return tableSchemaBuilder.build();
    }

    private static ColumnSchema getFieldColumnSchema(Field field, @Nullable StructType structTypeInSchema) {
        String name = field.getName();
        Annotation annotation = getAnnotationIfPresent(field, JavaPersistenceApi.columnAnnotations()).orElse(null);
        if (JavaPersistenceApi.isColumnAnnotationPresent(annotation)) {
            var columnName = JavaPersistenceApi.getColumnName(annotation);
            name = columnName.isEmpty() ? name : columnName;
        }
        return getClassColumnSchema(
                field.getType(),
                name,
                getTypeParametersOfField(field),
                annotation,
                getStructMemberTiType(name, structTypeInSchema)
                        .orElse(null)
        );
    }

    private static <T> ColumnSchema getClassColumnSchema(Class<T> clazz,
                                                         String name,
                                                         List<Type> genericTypeParameters,
                                                         @Nullable Annotation annotation,
                                                         @Nullable TiType tiTypeInSchema) {
        boolean isNullable = true;
        if (JavaPersistenceApi.isColumnAnnotationPresent(annotation)) {
            isNullable = JavaPersistenceApi.isColumnNullable(annotation);
        }

        TiType tiType = getClassTiType(
                clazz,
                annotation,
                genericTypeParameters,
                tiTypeInSchema
        );

        if (isNullable && !clazz.isPrimitive()) {
            tiType = TiType.optional(tiType);
        }

        return new ColumnSchema(name, tiType);
    }

    private static Optional<TiType> getStructMemberTiType(String name, @Nullable StructType structType) {
        return Optional.ofNullable(structType)
                .flatMap(
                        s -> s.getMembers().stream()
                                .filter(member -> member.getName().equals(name))
                                .map(StructType.Member::getType)
                                .findAny()
                ).map(
                        tiType -> tiType.isOptional() ?
                                tiType.asOptional().getItem() : tiType
                ).map(
                        tiType -> {
                            if (tiType.isOptional()) {
                                throw new RuntimeException("Table schema has column with optional<optional>");
                            }
                            return tiType;
                        }
                );
    }

    private static <T> TiType getClassTiType(Class<T> clazz,
                                             @Nullable Annotation annotation,
                                             List<Type> genericTypeParameters,
                                             @Nullable TiType tiTypeInSchema) {
        Optional<TiType> tiTypeIfSimple = getTiTypeIfSimple(
                clazz,
                JavaPersistenceApi.isColumnAnnotationPresent(annotation) ?
                        JavaPersistenceApi.getColumnDefinition(annotation) :
                        ""
        );
        if (Collection.class.isAssignableFrom(clazz)) {
            return getCollectionTiType(
                    genericTypeParameters.get(0),
                    Optional.ofNullable(tiTypeInSchema)
                            .filter(TiType::isList)
                            .map(TiType::asList)
                            .map(ListType::getItem)
                            .orElse(null)
            );
        }
        if (Map.class.isAssignableFrom(clazz)) {
            return getMapTiType(
                    genericTypeParameters.get(0),
                    genericTypeParameters.get(1),
                    Optional.ofNullable(tiTypeInSchema)
                            .filter(TiType::isDict)
                            .map(TiType::asDict)
                            .map(DictType::getKey)
                            .orElse(null),
                    Optional.ofNullable(tiTypeInSchema)
                            .filter(TiType::isDict)
                            .map(TiType::asDict)
                            .map(DictType::getValue)
                            .orElse(null)
            );
        }
        if (clazz.isArray()) {
            return getArrayTiType(
                    clazz,
                    Optional.ofNullable(tiTypeInSchema)
                            .filter(TiType::isList)
                            .map(TiType::asList)
                            .map(ListType::getItem)
                            .orElse(null)
            );
        }
        if (clazz.equals(BigDecimal.class)) {
            return getDecimalTiType(
                    annotation,
                    Optional.ofNullable(tiTypeInSchema)
                            .filter(TiType::isDecimal)
                            .map(TiType::asDecimal)
                            .orElse(null)
            );
        }
        return tiTypeIfSimple.orElseGet(() -> getEntityTiType(
                        clazz,
                        Optional.ofNullable(tiTypeInSchema)
                                .filter(TiType::isStruct)
                                .map(TiType::asStruct)
                                .orElse(null)
                )
        );
    }

    private static <T> TiType getEntityTiType(
            Class<T> clazz,
            @Nullable StructType structTypeInSchema
    ) {
        ArrayList<StructType.Member> members = new ArrayList<>();
        for (Field field : getAllDeclaredFields(clazz)) {
            if (isFieldTransient(field, JavaPersistenceApi.transientAnnotations())) {
                continue;
            }
            members.add(getStructMember(field, structTypeInSchema));
        }

        return TiType.struct(members);
    }

    private static StructType.Member getStructMember(
            Field field,
            @Nullable StructType structTypeInSchema
    ) {
        var columnSchema = getFieldColumnSchema(
                field,
                structTypeInSchema
        );
        return new StructType.Member(columnSchema.getName(), columnSchema.getTypeV3());
    }

    private static TiType getCollectionTiType(
            Type elementType,
            @Nullable TiType elementTiTypeInSchema
    ) {
        var elementTypeDescr = getTypeDescription(elementType);
        return TiType.list(
                getClassColumnSchema(
                        elementTypeDescr.getTypeClass(),
                        "",
                        elementTypeDescr.getTypeParameters(),
                        null,
                        elementTiTypeInSchema
                ).getTypeV3()
        );
    }

    private static TiType getMapTiType(
            Type keyType,
            Type valueType,
            @Nullable TiType keyTiTypeInSchema,
            @Nullable TiType valueTiTypeInSchema
    ) {
        var keyTypeDescr = getTypeDescription(keyType);
        var valueTypeDescr = getTypeDescription(valueType);
        return TiType.dict(
                getClassColumnSchema(
                        keyTypeDescr.getTypeClass(),
                        "",
                        keyTypeDescr.getTypeParameters(),
                        null,
                        keyTiTypeInSchema
                ).getTypeV3(),
                getClassColumnSchema(
                        valueTypeDescr.getTypeClass(),
                        "",
                        valueTypeDescr.getTypeParameters(),
                        null,
                        valueTiTypeInSchema
                ).getTypeV3()
        );
    }

    private static TiType getArrayTiType(
            Class<?> arrayClass,
            @Nullable TiType elementTiTypeInSchema
    ) {
        if (!arrayClass.isArray()) {
            throw new IllegalArgumentException("Argument must be array");
        }
        return TiType.list(
                getClassColumnSchema(
                        arrayClass.getComponentType(),
                        "",
                        List.of(),
                        null,
                        elementTiTypeInSchema
                ).getTypeV3()
        );
    }

    private static TiType getDecimalTiType(
            @Nullable Annotation annotation,
            @Nullable DecimalType decimalType
    ) {
        int precision = 0;
        int scale = 0;
        if (annotation != null &&
                anyMatchWithAnnotation(annotation, JavaPersistenceApi.columnAnnotations())) {
            precision = JavaPersistenceApi.getColumnPrecision(annotation);
            scale = JavaPersistenceApi.getColumnScale(annotation);
        }
        if (decimalType != null) {
            if (precision == 0 && scale == 0) {
                // precision and scale are not specified in annotation or set to default values
                precision = decimalType.getPrecision();
                scale = decimalType.getScale();
            } else if (precision != decimalType.getPrecision() || scale != decimalType.getScale()) {
                throw new MismatchEntityAndTableSchemaDecimalException();
            }
        }
        if (precision == 0 && scale == 0) {
            // no table schema and
            // precision and scale are not specified in annotation or set to default values
            throw new PrecisionAndScaleNotSpecifiedException();
        }
        return TiType.decimal(precision, scale);
    }

    public static class PrecisionAndScaleNotSpecifiedException extends RuntimeException {

    }

    public static class MismatchEntityAndTableSchemaDecimalException extends RuntimeException {

    }
}
