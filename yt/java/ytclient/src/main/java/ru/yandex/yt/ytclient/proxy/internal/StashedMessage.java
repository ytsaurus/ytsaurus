package ru.yandex.yt.ytclient.proxy.internal;

public class StashedMessage<DataType> {
    DataType data;
    long offset;

    StashedMessage(DataType data, long offset) {
        this.data = data;
        this.offset = offset;
    }
}
