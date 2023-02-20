package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class AlterTable extends tech.ytsaurus.client.request.AlterTable.BuilderBase<AlterTable> {
    public AlterTable(YPath path) {
        setPath(path);
    }

    /**
     * @deprecated Use {@link #AlterTable(YPath path)} instead.
     */
    @Deprecated
    public AlterTable(String path) {
        setPath(path);
    }

    @Override
    protected AlterTable self() {
        return this;
    }
}
