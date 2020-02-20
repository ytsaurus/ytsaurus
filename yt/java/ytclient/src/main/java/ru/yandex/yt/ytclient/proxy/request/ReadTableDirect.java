package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.ytclient.object.WireRowDeserializer;

public class ReadTableDirect extends ReadTable<byte[]> {
    public ReadTableDirect(String path) {
        super(path, (WireRowDeserializer<byte[]>) null);
    }
}
