package ru.yandex.yt.ytclient.tables;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.ytree.YTreeConvertible;

/**
 * TColumnSchema (yt/ytlib/table_client/schema.h)
 */
public class ColumnSchema implements YTreeConvertible {
    private final String name;
    private final ColumnValueType type;
    private final ColumnSortOrder sortOrder;
    private final String lock;
    private final String expression;
    private final String aggregate;
    private final String group;
    private final boolean required;

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

    public ColumnSchema(String name, ColumnValueType type, ColumnSortOrder sortOrder, String lock, String expression,
            String aggregate, String group, boolean required)
    {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.sortOrder = sortOrder;
        this.lock = lock;
        this.expression = expression;
        this.aggregate = aggregate;
        this.group = group;
        this.required = required;
    }

    public String getName() {
        return name;
    }

    public ColumnValueType getType() {
        return type;
    }

    public ColumnSortOrder getSortOrder() {
        return sortOrder;
    }

    public String getLock() {
        return lock;
    }

    public String getExpression() {
        return expression;
    }

    public String getAggregate() {
        return aggregate;
    }

    public String getGroup() {
        return group;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public YTreeNode toYTree() {
        YTreeBuilder builder = YTree.builder()
                .beginMap()
                .key("name").value(name)
                .key("type").value(type.getName())
                .key("required").value(required);
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
        ColumnValueType type = ColumnValueType.fromName(map.getOrThrow("type").stringValue());
        ColumnSortOrder sortOrder =
                map.get("sort_order").map(YTreeNode::stringValue).map(ColumnSortOrder::fromName)
                        .getOrNull();
        String lock = map.get("lock").map(YTreeNode::stringValue).getOrNull();
        String expression = map.get("expression").map(YTreeNode::stringValue).getOrNull();
        String aggregate = map.get("aggregate").map(YTreeNode::stringValue).getOrNull();
        String group = map.get("group").map(YTreeNode::stringValue).getOrNull();
        boolean required = map.get("required").map(YTreeNode::boolValue).getOrElse(false);
        return new ColumnSchema(name, type, sortOrder, lock, expression, aggregate, group, required);
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

        if (required != that.required) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (sortOrder != that.sortOrder) {
            return false;
        }
        if (lock != null ? !lock.equals(that.lock) : that.lock != null) {
            return false;
        }
        if (expression != null ? !expression.equals(that.expression) : that.expression != null) {
            return false;
        }
        if (aggregate != null ? !aggregate.equals(that.aggregate) : that.aggregate != null) {
            return false;
        }
        return group != null ? group.equals(that.group) : that.group == null;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (sortOrder != null ? sortOrder.hashCode() : 0);
        result = 31 * result + (lock != null ? lock.hashCode() : 0);
        result = 31 * result + (expression != null ? expression.hashCode() : 0);
        result = 31 * result + (aggregate != null ? aggregate.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (required ? 1 : 0);
        return result;
    }

    public static class Builder {
        private final String name;
        private final ColumnValueType type;
        private ColumnSortOrder sortOrder;
        private String lock;
        private String expression;
        private String aggregate;
        private String group;
        private boolean required;

        public Builder(String name, ColumnValueType type) {
            this.name = name;
            this.type = type;
        }

        public Builder(String name, ColumnValueType type, boolean required) {
            this.name = name;
            this.type = type;
            this.required = required;
        }

        public Builder(ColumnSchema columnSchema) {
            this.name = columnSchema.name;
            this.type = columnSchema.type;
            this.sortOrder = columnSchema.sortOrder;
            this.lock = columnSchema.lock;
            this.expression = columnSchema.expression;
            this.aggregate = columnSchema.aggregate;
            this.group = columnSchema.group;
            this.required = columnSchema.required;
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
            return new ColumnSchema(name, type, sortOrder, lock, expression, aggregate, group, required);
        }
    }
}
