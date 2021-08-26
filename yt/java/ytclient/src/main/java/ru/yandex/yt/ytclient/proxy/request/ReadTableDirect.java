package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.ytclient.object.WireRowDeserializer;

public class ReadTableDirect extends ReadTable<byte[]> {
    public ReadTableDirect(YPath path) {
        super(path, (WireRowDeserializer<byte[]>) null);
    }

    /**
     * @deprecated Use {@link #ReadTableDirect(YPath path)} instead.
     */
    @Deprecated
    public ReadTableDirect(String path) {
        super(path, (WireRowDeserializer<byte[]>) null);
    }
}
