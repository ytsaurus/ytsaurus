package tech.ytsaurus.client.rows;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.StructType;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.typeinfo.TypeName;
import tech.ytsaurus.ysontree.YTreeNode;

class TiTypeUtil {
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
            Map.entry(String.class, TiType.string())
    );

    private static final Map<String, TiType> COLUMN_DEFINITION_TO_TI_TYPE_MAP = Map.ofEntries(
            Map.entry(TypeName.Uint8.getWireName(), TiType.uint8()),
            Map.entry(TypeName.Uint16.getWireName(), TiType.uint16()),
            Map.entry(TypeName.Uint32.getWireName(), TiType.uint32()),
            Map.entry(TypeName.Uint64.getWireName(), TiType.uint64())
    );

    private static final String COLUMN_DEFINITION_EXCEPTION_MESSAGE_FORMAT =
            "Field with columnDefinition='%s' must be of type '%s'";
    private static final String LONG_NAME = "Long";

    private TiTypeUtil() {
    }

    static Optional<TiType> getTiTypeIfSimple(Class<?> clazz, String columnDefinition) {
        for (var stringTiTypeEntry : COLUMN_DEFINITION_TO_TI_TYPE_MAP.entrySet()) {
            if (!columnDefinition.equalsIgnoreCase(stringTiTypeEntry.getKey())) {
                continue;
            }
            if (!clazz.equals(Long.class)) {
                throw new RuntimeException(
                        String.format(
                                COLUMN_DEFINITION_EXCEPTION_MESSAGE_FORMAT,
                                stringTiTypeEntry.getKey(),
                                LONG_NAME
                        )
                );
            }
            return Optional.of(stringTiTypeEntry.getValue());
        }
        if (YTreeNode.class.isAssignableFrom(clazz)) {
            return Optional.of(TiType.yson());
        }
        return Optional.ofNullable(SIMPLE_TYPES_MAP.get(clazz));
    }

    static boolean isSimpleType(TiType tiType) {
        return tiType.isInt8() || tiType.isInt16() ||
                tiType.isInt32() || tiType.isInt64() ||
                tiType.isUint8() || tiType.isUint16() ||
                tiType.isUint32() || tiType.isUint64() ||
                tiType.isDouble() || tiType.isBool() ||
                tiType.isString() || tiType.isYson() ||
                tiType.isNull();
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
