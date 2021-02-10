package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

/**
 * Unmount table request.
 *
 * @see <a href="https://yt.yandex-team.ru/docs/api/commands#unmount_table">documentation</a>
 * @see ru.yandex.yt.ytclient.proxy.ApiServiceClient#unmountTable(UnmountTable)
 */
@NonNullApi
public class UnmountTable extends TableReq<UnmountTable> implements HighLevelRequest<TReqUnmountTable.Builder> {
    boolean force = false;

    public UnmountTable(String path) {
        super(path);
    }

    /**
     * Force unmount.
     *
     * <b>Dangerous:</b> this flag should not be used unless you understand how it works.
     * Might lead to data loss.
     */
    public UnmountTable setForce(boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUnmountTable.Builder, ?> builder) {
        super.writeTo(builder.body());
        builder.body().setForce(force);
    }

    @Override
    protected UnmountTable self() {
        return this;
    }

}
