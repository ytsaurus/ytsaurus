package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;

public class ReshardTable extends ru.yandex.yt.ytclient.request.ReshardTable.BuilderBase<
        ReshardTable, ru.yandex.yt.ytclient.request.ReshardTable> {
    public ReshardTable(YPath path) {
        setPath(path.justPath());
    }

    /**
     * @deprecated Use {@link #ReshardTable(YPath path)} instead.
     */
    @Deprecated
    public ReshardTable(String path) {
        setPath(path);
    }

    @Override
    protected ReshardTable self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.ReshardTable build() {
        return new ru.yandex.yt.ytclient.request.ReshardTable(this);
    }
}
