package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.rpcproxy.EOperationType;
import tech.ytsaurus.ysontree.YTreeNode;

public class StartOperation extends tech.ytsaurus.client.request.StartOperation.BuilderBase<StartOperation> {
    public StartOperation(EOperationType type, YTreeNode spec) {
        setType(type).setSpec(spec);
    }

    @Override
    protected StartOperation self() {
        return this;
    }
}
