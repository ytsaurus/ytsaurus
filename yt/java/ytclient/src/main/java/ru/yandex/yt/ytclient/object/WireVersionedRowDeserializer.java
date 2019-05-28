package ru.yandex.yt.ytclient.object;

import java.util.List;

import ru.yandex.yt.ytclient.wire.WireColumnSchema;

public interface WireVersionedRowDeserializer<T> {

    void onWriteTimestamps(List<Long> timestamps);

    void onDeleteTimestamps(List<Long> timestamps);

    List<WireColumnSchema> getKeyColumnSchema();

    WireValueDeserializer<?> keys(int keyCount);

    WireValueDeserializer<?> values(int valueCount);

    T onCompleteRow();
}
