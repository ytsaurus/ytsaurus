package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;

@NonNullApi
public class MountTable extends ru.yandex.yt.ytclient.request.MountTable.BuilderBase<MountTable> {
    public MountTable(YPath path) {
        setPath(path.justPath());
    }

    /**
     * @deprecated Use {@link #MountTable(YPath path)} instead.
     */
    @Deprecated
    public MountTable(String path) {
        setPath(path);
    }

    @Override
    protected MountTable self() {
        return this;
    }
}
