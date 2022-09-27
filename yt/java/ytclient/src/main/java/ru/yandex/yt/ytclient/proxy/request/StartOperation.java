package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.EOperationType;

public class StartOperation extends ru.yandex.yt.ytclient.request.StartOperation.BuilderBase<
        StartOperation, ru.yandex.yt.ytclient.request.StartOperation> {
    public StartOperation(EOperationType type, YTreeNode spec) {
        setType(type).setSpec(spec);
    }

    @Override
    protected StartOperation self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.StartOperation build() {
        return new ru.yandex.yt.ytclient.request.StartOperation(this);
    }
}
