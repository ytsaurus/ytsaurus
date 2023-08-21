package tech.ytsaurus.client.rows;

import java.util.Objects;


import tech.ytsaurus.core.tables.ColumnValueType;

public class WireColumnSchema {
    private final int id;
    private final ColumnValueType type;
    private final boolean aggregate;

    public WireColumnSchema(int id, ColumnValueType type) {
        this(id, type, false);
    }

    public WireColumnSchema(int id, ColumnValueType type, boolean aggregate) {
        this.id = id;
        this.type = Objects.requireNonNull(type);
        this.aggregate = aggregate;
    }

    public int getId() {
        return id;
    }

    public ColumnValueType getType() {
        return type;
    }

    public boolean isAggregate() {
        return aggregate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WireColumnSchema)) {
            return false;
        }

        WireColumnSchema that = (WireColumnSchema) o;

        if (id != that.id) {
            return false;
        }
        if (aggregate != that.aggregate) {
            return false;
        }
        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + type.hashCode();
        result = 31 * result + (aggregate ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WireColumnSchema{" +
                "id=" + id +
                ", type=" + type +
                ", aggregate=" + aggregate +
                '}';
    }
}
