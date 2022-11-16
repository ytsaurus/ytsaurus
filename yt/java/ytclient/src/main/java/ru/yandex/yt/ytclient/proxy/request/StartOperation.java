package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.yt.rpcproxy.EOperationType;

public class StartOperation extends tech.ytsaurus.client.request.StartOperation.BuilderBase<StartOperation> {
    public StartOperation(EOperationType type, YTreeNode spec) {
        setType(type).setSpec(spec);
    }

    @Override
    protected StartOperation self() {
        return this;
    }
}
