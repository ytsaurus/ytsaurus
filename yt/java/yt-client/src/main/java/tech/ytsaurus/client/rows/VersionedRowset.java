package tech.ytsaurus.client.rows;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeMapNode;


public class VersionedRowset {
    private final TableSchema schema;
    private final List<VersionedRow> rows;

    public VersionedRowset(TableSchema schema, List<VersionedRow> rows) {
        this.schema = Objects.requireNonNull(schema);
        this.rows = Objects.requireNonNull(rows);
    }

    public TableSchema getSchema() {
        return schema;
    }

    public List<VersionedRow> getRows() {
        return Collections.unmodifiableList(rows);
    }

    public List<YTreeMapNode> getYTreeRows() {
        return new AbstractList<YTreeMapNode>() {
            @Override
            public YTreeMapNode get(int index) {
                VersionedRow row = rows.get(index);
                return row != null ? row.toYTreeMap(schema) : null;
            }

            @Override
            public int size() {
                return rows.size();
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VersionedRowset)) {
            return false;
        }

        VersionedRowset that = (VersionedRowset) o;

        if (!schema.equals(that.schema)) {
            return false;
        }
        return rows.equals(that.rows);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + rows.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "VersionedRowset{" +
                "schema=" + schema +
                ", rows=" + rows +
                '}';
    }
}
