package ru.yandex.yt.ytclient.object;

public interface WireVersionedRowsetDeserializer<T> extends WireVersionedRowDeserializer<T> {

    void setRowCount(int rowCount);
}
