package tech.ytsaurus.client.rows;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.rows.YsonSerializable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.StructType;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeNode;

class TiTypeUtil {
    private static class TypeDescription {
        private final TiType tiType;
        private final Set<Class<?>> fieldTypes;

        TypeDescription(TiType tiType, Class<?>... fieldTypes) {
            this.tiType = tiType;
            this.fieldTypes = Set.of(fieldTypes);
        }

        public TiType getTiType() {
            return tiType;
        }

        public Set<Class<?>> getFieldTypes() {
            return fieldTypes;
        }
    }


    private static class ColumnDefinitionToTypeDescriptionMapper {
        private final Map<String, TypeDescription> columnDefinitionToTypeDescriptionMap;

        ColumnDefinitionToTypeDescriptionMapper(TypeDescription... typeDescriptions) {
            this.columnDefinitionToTypeDescriptionMap = Arrays.stream(typeDescriptions)
                    .collect(Collectors.toMap(
                            typeDescr -> typeDescr.getTiType().getTypeName().getWireName(), Function.identity()
                    ));
        }

        boolean hasTypeDescription(String columnDefinition) {
            return columnDefinitionToTypeDescriptionMap.containsKey(columnDefinition);
        }

        TypeDescription getTypeDescription(String columnDefinition) {
            return columnDefinitionToTypeDescriptionMap.get(columnDefinition);
        }
    }

    private static final ColumnDefinitionToTypeDescriptionMapper COLUMN_DEFINITION_TO_TYPE_DESCR =
            new ColumnDefinitionToTypeDescriptionMapper(
                    new TypeDescription(TiType.uint8(), long.class, Long.class),
                    new TypeDescription(TiType.uint16(), long.class, Long.class),
                    new TypeDescription(TiType.uint32(), long.class, Long.class),
                    new TypeDescription(TiType.uint64(), long.class, Long.class),
                    new TypeDescription(TiType.string(), String.class, Enum.class),
                    new TypeDescription(TiType.int16(), byte.class, Byte.class),
                    new TypeDescription(TiType.int32(), short.class, Short.class, byte.class, Byte.class),
                    new TypeDescription(
                            TiType.int64(), int.class, Integer.class, short.class, Short.class,
                            byte.class, Byte.class, Instant.class
                    )
            );

    private static final Map<Class<?>, TiType> SIMPLE_TYPES_MAP = Map.ofEntries(
            Map.entry(byte.class, TiType.int8()),
            Map.entry(Byte.class, TiType.int8()),
            Map.entry(short.class, TiType.int16()),
            Map.entry(Short.class, TiType.int16()),
            Map.entry(int.class, TiType.int32()),
            Map.entry(Integer.class, TiType.int32()),
            Map.entry(long.class, TiType.int64()),
            Map.entry(Long.class, TiType.int64()),
            Map.entry(double.class, TiType.doubleType()),
            Map.entry(Double.class, TiType.doubleType()),
            Map.entry(boolean.class, TiType.bool()),
            Map.entry(Boolean.class, TiType.bool()),
            Map.entry(String.class, TiType.utf8()),
            Map.entry(byte[].class, TiType.string()),
            Map.entry(GUID.class, TiType.uuid()),
            Map.entry(Instant.class, TiType.timestamp())
    );

    private TiTypeUtil() {
    }

    static Optional<TiType> getTiTypeIfSimple(Class<?> clazz) {
        if (YTreeNode.class.isAssignableFrom(clazz) || YsonSerializable.class.isAssignableFrom(clazz)) {
            return Optional.of(TiType.yson());
        }
        if (Enum.class.isAssignableFrom(clazz)) {
            return Optional.of(TiType.utf8());
        }
        return Optional.ofNullable(SIMPLE_TYPES_MAP.get(clazz));
    }

    static TiType getTiTypeForClassWithColumnDefinition(Class<?> clazz, String columnDefinition) {
        if (COLUMN_DEFINITION_TO_TYPE_DESCR.hasTypeDescription(columnDefinition)) {
            var typeDescr = COLUMN_DEFINITION_TO_TYPE_DESCR.getTypeDescription(columnDefinition);
            if (typeDescr.getFieldTypes().stream().noneMatch(typeClass -> typeClass.isAssignableFrom(clazz))) {
                throw new RuntimeException(
                        String.format(
                                "Field with columnDefinition='%s' must be one of the following types: '%s'",
                                columnDefinition,
                                typeDescr.getFieldTypes()
                        )
                );
            }
            return typeDescr.getTiType();
        }
        throw new RuntimeException(
                String.format("columnDefinition='%s' is not supported", columnDefinition)
        );
    }

    static boolean isSimpleType(TiType tiType) {
        return tiType.isInt8() || tiType.isInt16() ||
                tiType.isInt32() || tiType.isInt64() ||
                tiType.isUint8() || tiType.isUint16() ||
                tiType.isUint32() || tiType.isUint64() ||
                tiType.isDouble() || tiType.isBool() ||
                tiType.isUtf8() || tiType.isString() ||
                tiType.isUuid() || tiType.isYson() ||
                tiType.isTimestamp();
    }

    static TiType tableSchemaToStructTiType(TableSchema tableSchema) {
        return TiType.struct(
                tableSchema.getColumns().stream()
                        .map(columnSchema -> new StructType.Member(
                                columnSchema.getName(), columnSchema.getTypeV3()
                        ))
                        .collect(Collectors.toList())
        );
    }
}
