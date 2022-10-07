package ru.yandex.yt.ytclient.object;

public interface WireRowsetDeserializer<T> extends WireRowDeserializer<T> {
    void setRowCount(int rowCount);
}
