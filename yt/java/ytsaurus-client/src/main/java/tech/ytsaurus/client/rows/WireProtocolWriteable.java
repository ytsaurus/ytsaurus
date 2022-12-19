package tech.ytsaurus.client.rows;


import tech.ytsaurus.core.tables.ColumnValueType;

public interface WireProtocolWriteable extends YTreeConsumable {

    void writeValueCount(int valueCount);

    void writeValueHeader(int columnId, ColumnValueType type, boolean aggregate, int length);

    void overwriteValueCount(int valueCount);

}
