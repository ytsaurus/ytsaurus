package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class AlterTable extends ru.yandex.yt.ytclient.request.AlterTable.BuilderBase<AlterTable> {
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
