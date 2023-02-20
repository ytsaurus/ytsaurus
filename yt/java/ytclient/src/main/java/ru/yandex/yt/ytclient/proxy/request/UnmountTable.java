package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;

/**
 * Unmount table request.
 *
 * @see <a href="https://yt.yandex-team.ru/docs/api/commands#unmount_table">documentation</a>
 * @see ru.yandex.yt.ytclient.proxy.ApiServiceClient#unmountTable(UnmountTable)
 */
@NonNullApi
public class UnmountTable extends tech.ytsaurus.client.request.UnmountTable.BuilderBase<UnmountTable> {
    public UnmountTable(YPath path) {
        setPath(path.justPath());
    }

    /**
     * @deprecated Use {@link #UnmountTable(YPath path)} instead.
     */
    @Deprecated
    public UnmountTable(String path) {
        setPath(path);
    }

    @Override
    protected UnmountTable self() {
        return this;
    }
}
