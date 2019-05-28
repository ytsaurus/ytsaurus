package ru.yandex.yt.ytclient.object;

public interface WireRowDeserializer<T> {

    WireValueDeserializer<?> onNewRow(int columnCount);

    T onCompleteRow();
}
