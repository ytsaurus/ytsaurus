package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.yt.rpcproxy.TReqCheckClusterLiveness;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class CheckClusterLiveness extends RequestBase<CheckClusterLiveness>
        implements HighLevelRequest<TReqCheckClusterLiveness.Builder> {
    private boolean checkCypressRoot = false;
    private boolean checkSecondaryMasterCells = false;

    public CheckClusterLiveness() {
    }

    /*
     Checks cypress root availability.
     */
    public CheckClusterLiveness setCheckCypressRoot(boolean checkCypressRoot) {
        this.checkCypressRoot = checkCypressRoot;
        return this;
    }

    /*
    Checks secondary master cells generic availability.
     */
    public CheckClusterLiveness setCheckSecondaryMasterCells(boolean checkSecondaryMasterCells) {
        this.checkSecondaryMasterCells = checkSecondaryMasterCells;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCheckClusterLiveness.Builder, ?> builder) {
        builder.body().setCheckCypressRoot(checkCypressRoot);
        builder.body().setCheckSecondaryMasterCells(checkSecondaryMasterCells);
    }

    @Nonnull
    @Override
    protected CheckClusterLiveness self() {
        return this;
    }
}
