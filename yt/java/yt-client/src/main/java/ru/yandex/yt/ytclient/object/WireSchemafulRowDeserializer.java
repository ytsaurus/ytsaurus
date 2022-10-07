package ru.yandex.yt.ytclient.object;

import java.util.List;

import ru.yandex.yt.ytclient.wire.WireColumnSchema;

public interface WireSchemafulRowDeserializer<T> extends WireRowDeserializer<T> {

    List<WireColumnSchema> getColumnSchema();
}
