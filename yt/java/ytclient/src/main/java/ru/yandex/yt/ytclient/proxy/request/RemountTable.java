package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
public class RemountTable extends tech.ytsaurus.client.request.RemountTable.BuilderBase<RemountTable> {
    public RemountTable(YPath path) {
        setPath(path.justPath());
    }

    /**
     * @deprecated Use {@link #RemountTable(YPath path)} instead.
     */
    @Deprecated
    public RemountTable(String path) {
        setPath(path);
    }

    @Override
    protected RemountTable self() {
        return this;
    }
}
