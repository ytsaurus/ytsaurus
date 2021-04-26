package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.yt.rpcproxy.TReqReshardTableAutomatic;

public class ReshardTableAutomatic extends TableReq<ReshardTableAutomatic> {
    private boolean keepActions = false;

    public ReshardTableAutomatic(String path, boolean keepActions) {
        super(path);
        this.keepActions = keepActions;
    }

    public ReshardTableAutomatic setKeepActions(boolean keepActions) {
        this.keepActions = keepActions;
        return this;
    }

    public TReqReshardTableAutomatic.Builder writeTo(TReqReshardTableAutomatic.Builder builder) {
        super.writeTo(builder);
        builder.setKeepActions(keepActions);
        return builder;
    }

    @Nonnull
    @Override
    protected ReshardTableAutomatic self() {
        return this;
    }
}
