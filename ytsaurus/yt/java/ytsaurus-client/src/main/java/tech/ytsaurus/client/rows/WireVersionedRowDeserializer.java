package tech.ytsaurus.client.rows;

import java.util.List;

public interface WireVersionedRowDeserializer<T> {

    void onWriteTimestamps(List<Long> timestamps);

    void onDeleteTimestamps(List<Long> timestamps);

    List<WireColumnSchema> getKeyColumnSchema();

    WireValueDeserializer<?> keys(int keyCount);

    WireValueDeserializer<?> values(int valueCount);

    T onCompleteRow();
}
