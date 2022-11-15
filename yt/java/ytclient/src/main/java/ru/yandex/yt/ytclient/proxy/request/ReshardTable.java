package ru.yandex.yt.ytclient.proxy.request;


import tech.ytsaurus.core.cypress.YPath;
public class ReshardTable extends ru.yandex.yt.ytclient.request.ReshardTable.BuilderBase<ReshardTable> {
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
}
