package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
public class UnfreezeTable extends tech.ytsaurus.client.request.UnfreezeTable.BuilderBase<UnfreezeTable> {
    public UnfreezeTable(YPath path) {
        setPath(path);
    }

    /**
     * @deprecated Use {@link #UnfreezeTable(YPath path)} instead.
     */
    @Deprecated
    public UnfreezeTable(String path) {
        setPath(path);
    }

    @Override
    protected UnfreezeTable self() {
        return this;
    }
}
