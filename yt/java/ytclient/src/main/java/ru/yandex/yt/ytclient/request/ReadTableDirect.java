package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;

public class ReadTableDirect extends ReadTable<byte[]> {
    public ReadTableDirect(YPath path) {
        super(path, new SerializationContext<>());
    }

    /**
     * @deprecated Use {@link #ReadTableDirect(YPath path)} instead.
     */
    @Deprecated
    public ReadTableDirect(String path) {
        super(ReadTable.<byte[]>builder().setPath(path).setSerializationContext(new SerializationContext<>()));
    }
}
