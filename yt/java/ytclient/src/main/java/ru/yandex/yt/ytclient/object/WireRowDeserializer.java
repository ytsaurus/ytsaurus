package ru.yandex.yt.ytclient.object;

import ru.yandex.yt.ytclient.tables.TableSchema;

public interface WireRowDeserializer<T> {

    WireValueDeserializer<?> onNewRow(int columnCount);

    T onCompleteRow();

    default void updateSchema(TableSchema schema) { }
}
