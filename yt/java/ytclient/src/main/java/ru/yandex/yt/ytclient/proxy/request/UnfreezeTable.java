package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;

@NonNullApi
public class UnfreezeTable extends ru.yandex.yt.ytclient.request.UnfreezeTable.BuilderBase<
        UnfreezeTable, ru.yandex.yt.ytclient.request.UnfreezeTable> {
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

    @Override
    public ru.yandex.yt.ytclient.request.UnfreezeTable build() {
        return new ru.yandex.yt.ytclient.request.UnfreezeTable(this);
    }
}
