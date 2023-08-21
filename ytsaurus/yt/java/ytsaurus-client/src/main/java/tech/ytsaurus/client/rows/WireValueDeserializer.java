package tech.ytsaurus.client.rows;


import tech.ytsaurus.core.tables.ColumnValueType;

public interface WireValueDeserializer<T> extends YTreeConsumable {

    void setId(int id);

    void setType(ColumnValueType type);

    void setAggregate(boolean aggregate);

    void setTimestamp(long timestamp);

    T build();
}
