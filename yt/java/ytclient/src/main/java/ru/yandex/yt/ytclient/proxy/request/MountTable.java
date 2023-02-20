package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
public class MountTable extends tech.ytsaurus.client.request.MountTable.BuilderBase<MountTable> {
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
