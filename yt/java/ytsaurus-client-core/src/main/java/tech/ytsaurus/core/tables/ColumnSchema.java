package tech.ytsaurus.core.tables;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.typeinfo.TypeIO;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;


/**
 * TColumnSchema (yt/ytlib/table_client/schema.h)
 */
@NonNullFields
public class ColumnSchema implements YTreeConvertible {
    private final String name;
    private final TiType typeV3;
    private final @Nullable ColumnSortOrder sortOrder;
    private final @Nullable String lock;
    private final @Nullable String expression;
    private final @Nullable String aggregate;
    private final @Nullable String group;

    public ColumnSchema(String name, TiType typeV3) {
        this(name, typeV3, null);
    }

    public ColumnSchema(String name, TiType typeV3, @Nullable ColumnSortOrder sortOrder) {
        this.name = name;
        this.typeV3 = typeV3;
        this.sortOrder = sortOrder;
        this.lock = null;
        this.expression = null;
        this.aggregate = null;
        this.group = null;
    }

    public ColumnSchema(String name, ColumnValueType type) {
        this(name, type, null, null, null, null, null, false);
    }

    public ColumnSchema(String name, ColumnValueType type, ColumnSortOrder sortOrder) {
        this(name, type, sortOrder, null, null, null, null, false);
    }

    /**
     * @deprecated this constructor lacks the {@code required} parameter, use the full version
     */
    @Deprecated
    public ColumnSchema(String name, ColumnValueType type, ColumnSortOrder sortOrder, String lock, String expression,
            String aggregate, String group) {
        this(name, type, sortOrder, lock, expression, aggregate, group, false);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public ColumnSchema(
            String name,
            ColumnValueType type,
            @Nullable ColumnSortOrder sortOrder,
            @Nullable String lock,
            @Nullable String expression,
            @Nullable String aggregate,
            @Nullable String group,
            boolean required
    ) {
        this.name = Objects.requireNonNull(name);
        this.typeV3 = fromOldType(type, required);
        this.sortOrder = sortOrder;
        this.lock = lock;
        this.expression = expression;
        this.aggregate = aggregate;
        this.group = group;
    }

    private ColumnSchema(Builder builder) {
        this.name = builder.name;
        this.typeV3 = builder.typeV3;
        this.sortOrder = builder.sortOrder;
        this.lock = builder.lock;
        this.expression = builder.expression;
        this.aggregate = builder.aggregate;
        this.group = builder.group;
    }

    public static Builder builder(String name, TiType typeV3) {
        return new Builder(name, typeV3);
    }

    public static Builder builder(String name, ColumnValueType type) {
        return new Builder(name, type);
    }

    public static Builder builder(String name, ColumnValueType type, boolean required) {
        return new Builder(name, type, required);
    }

    public String getName() {
        return name;
    }

    public ColumnValueType getType() {
        return toOldType(typeV3).columnValueType;
    }

    public TiType getTypeV3() {
        return typeV3;
    }

    public @Nullable ColumnSortOrder getSortOrder() {
        return sortOrder;
    }

    public @Nullable String getLock() {
        return lock;
    }

    public @Nullable String getExpression() {
        return expression;
    }

    public @Nullable String getAggregate() {
        return aggregate;
    }

    public @Nullable String getGroup() {
        return group;
    }

    public boolean isRequired() {
        return toOldType(typeV3).required;
    }

    @Override
    public YTreeNode toYTree() {
        YTreeBuilder builder = YTree.builder()
                .beginMap()
                .key("name").value(name)
                .key("type_v3").apply(b -> {
                    typeV3.serializeTo(b);
                    return b;
                });
        if (sortOrder != null) {
            builder.key("sort_order").value(sortOrder.getName());
        }
        if (lock != null) {
            builder.key("lock").value(lock);
        }
        if (expression != null) {
            builder.key("expression").value(expression);
        }
        if (aggregate != null) {
            builder.key("aggregate").value(aggregate);
        }
        if (group != null) {
            builder.key("group").value(group);
        }
        return builder.buildMap();
    }

    public static ColumnSchema fromYTree(YTreeNode node) {
        YTreeMapNode map = node.mapNode();
        String name = map.getOrThrow("name").stringValue();
        YTreeNode typeV3Node = map.get("type_v3").orElse(null);
        TiType typeV3;
        if (typeV3Node != null) {
            typeV3 = TypeIO.parseYson(c -> YTreeNodeUtils.walk(
                    map.getOrThrow("type_v3"),
                    c,
                    true)
            );
        } else {
            boolean required = map.get("required").map(YTreeNode::boolValue).orElse(false);
            ColumnValueType type = ColumnValueType.fromName(
                    map.getOrThrow(
                            "type",
                            () -> "Neither 'type_v3' nor 'type' is specified"
                    ).stringValue());
            typeV3 = fromOldType(type, required);
        }
        ColumnSortOrder sortOrder =
                map.get("sort_order").map(YTreeNode::stringValue).map(ColumnSortOrder::fromName)
                        .orElse(null);
        return ColumnSchema.builder(name, typeV3)
                .setSortOrder(sortOrder)
                .setLock(map.get("lock").map(YTreeNode::stringValue).orElse(null))
                .setExpression(map.get("expression").map(YTreeNode::stringValue).orElse(null))
                .setAggregate(map.get("aggregate").map(YTreeNode::stringValue).orElse(null))
                .setGroup(map.get("group").map(YTreeNode::stringValue).orElse(null))
                .build();
    }

    public ColumnSchema.Builder toBuilder() {
        return new ColumnSchema.Builder(this);
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnSchema)) {
            return false;
        }

        ColumnSchema that = (ColumnSchema) o;

        return name.equals(that.name) &&
                typeV3.equals(that.typeV3) &&
                sortOrder == that.sortOrder &&
                Objects.equals(lock, that.lock) &&
                Objects.equals(expression, that.expression) &&
                Objects.equals(aggregate, that.aggregate) &&
                Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                typeV3,
                sortOrder,
                lock,
                expression,
                aggregate,
                group
        );
    }

    static class OldType {
        ColumnValueType columnValueType;
        boolean required;

        OldType(ColumnValueType columnValueType, boolean required) {
            this.columnValueType = columnValueType;
            this.required = required;
        }
    }

    static TiType fromOldType(ColumnValueType columnValueType, boolean required) {
        TiType newType;
        switch (columnValueType) {
            case NULL:
                if (required) {
                    throw new IllegalStateException("Type " + columnValueType + " cannot be required");
                }
                return TiType.nullType();
            case INT64:
                newType = TiType.int64();
                break;
            case UINT64:
                newType = TiType.uint64();
                break;
            case DOUBLE:
                newType = TiType.doubleType();
                break;
            case BOOLEAN:
                newType = TiType.bool();
                break;
            case STRING:
                newType = TiType.string();
                break;
            case ANY:
                newType = TiType.yson();
                break;
            case MAX:
            case MIN:
            case THE_BOTTOM:
            default:
                throw new IllegalStateException("Cannot convert " + columnValueType);
        }
        if (required) {
            return newType;
        } else {
            return TiType.optional(newType);
        }
    }

    static OldType toOldType(TiType type) {
        switch (type.getTypeName()) {
            case Bool:
                return new OldType(ColumnValueType.BOOLEAN, true);
            case Int8:
            case Int16:
            case Int32:
            case Int64:
            case Interval:
                return new OldType(ColumnValueType.INT64, true);
            case Uint8:
            case Uint16:
            case Uint32:
            case Uint64:

            case Date:
            case Datetime:
            case Timestamp:
                return new OldType(ColumnValueType.UINT64, true);
            case Float:
            case Double:
                return new OldType(ColumnValueType.DOUBLE, true);
            case String:
            case Utf8:
            case Decimal:
            case Json:
            case Uuid:
                return new OldType(ColumnValueType.STRING, true);

            case Void:
            case Null:
                return new OldType(ColumnValueType.NULL, false);

            case Optional:
                OldType itemOldType = toOldType(type.asOptional().getItem());
                if (itemOldType.required) {
                    return new OldType(itemOldType.columnValueType, false);
                } else {
                    return new OldType(ColumnValueType.ANY, false);
                }
            case Yson:
            case List:
            case Dict:
            case Struct:
            case Tuple:
            case Variant:
                return new OldType(ColumnValueType.ANY, true);
            case Tagged:
                return toOldType(type.asTaggedType().getItem());
            case TzDate:
            case TzDatetime:
            case TzTimestamp:
            case Extension:
            default:
                throw new IllegalStateException("Type " + type + " is not supported by YT");
        }

    }

    public static class Builder {
        private final String name;
        private final TiType typeV3;
        private @Nullable ColumnSortOrder sortOrder;
        private @Nullable String lock;
        private @Nullable String expression;
        private @Nullable String aggregate;
        private @Nullable String group;

        public Builder(String name, ColumnValueType type) {
            this.name = name;
            this.typeV3 = fromOldType(type, false);
        }

        public Builder(String name, ColumnValueType type, boolean required) {
            this.name = name;
            this.typeV3 = fromOldType(type, required);
        }

        public Builder(String name, TiType typeV3) {
            this.name = name;
            this.typeV3 = typeV3;
        }

        public Builder(ColumnSchema columnSchema) {
            this.name = columnSchema.name;
            this.typeV3 = columnSchema.typeV3;
            this.sortOrder = columnSchema.sortOrder;
            this.lock = columnSchema.lock;
            this.expression = columnSchema.expression;
            this.aggregate = columnSchema.aggregate;
            this.group = columnSchema.group;
        }

        public Builder setSortOrder(ColumnSortOrder sortOrder) {
            this.sortOrder = sortOrder;
            return this;
        }

        public Builder setLock(String lock) {
            this.lock = lock;
            return this;
        }

        public Builder setExpression(String expression) {
            this.expression = expression;
            return this;
        }

        public Builder setAggregate(String aggregate) {
            this.aggregate = aggregate;
            return this;
        }

        public Builder setGroup(String group) {
            this.group = group;
            return this;
        }

        public ColumnSchema build() {
            return new ColumnSchema(this);
        }
    }
}
