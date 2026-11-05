package tech.ytsaurus.flow.testutils;

import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

public class SchemaGenerator {
    private final String hashColumnName;
    private final String strColumnPrefix;
    private final String numColumnPrefix;

    SchemaGenerator(Builder builder) {
        this.hashColumnName = builder.hashColumnName;
        this.strColumnPrefix = builder.strColumnPrefix;
        this.numColumnPrefix = builder.numColumnPrefix;
    }

    public static Builder builder() {
        return new Builder();
    }

    public TableSchema createStringSchema(int columns) {
        var schema = TableSchema.builder()
                .addValue(hashColumnName, ColumnValueType.UINT64);
        for (int i = 0; i < columns; ++i) {
            schema.addValue(strColumnName(i), ColumnValueType.STRING);
        }
        return schema.build();
    }

    public TableSchema createNumberSchema(int columns) {
        var schema = TableSchema.builder()
                .addValue(hashColumnName, ColumnValueType.UINT64);
        for (int i = 0; i < columns; ++i) {
            schema.addValue(numColumnName(i), ColumnValueType.INT64);
        }
        return schema.build();
    }

    public TableSchema createMixedSchema(int columns) {
        var schema = TableSchema.builder()
                .addValue(hashColumnName, ColumnValueType.UINT64);
        int pairs = columns / 2;
        for (int i = 0; i < pairs; ++i) {
            schema.addValue(strColumnName(i), ColumnValueType.STRING);
            schema.addValue(numColumnName(i), ColumnValueType.INT64);
        }
        return schema.build();
    }

    public String strColumnName(int idx) {
        return strColumnPrefix + idx;
    }

    public String numColumnName(int idx) {
        return numColumnPrefix + idx;
    }

    public static class Builder {
        private String hashColumnName = "hashColumn";
        private String strColumnPrefix = "strColumn";
        private String numColumnPrefix = "numColumn";

        public Builder setHashColumnName(String name) {
            hashColumnName = name;
            return this;
        }

        public Builder setStrColumnPrefix(String prefix) {
            strColumnPrefix = prefix;
            return this;
        }

        public Builder setNumColumnPrefix(String prefix) {
            numColumnPrefix = prefix;
            return this;
        }

        public SchemaGenerator build() {
            return new SchemaGenerator(this);
        }
    }
}
