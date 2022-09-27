package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;

public class GetOperation extends ru.yandex.yt.ytclient.request.GetOperation.BuilderBase<
        GetOperation, ru.yandex.yt.ytclient.request.GetOperation> {
    public GetOperation(GUID guid) {
        setId(guid);
    }

    @Override
    protected GetOperation self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.GetOperation build() {
        return new ru.yandex.yt.ytclient.request.GetOperation(this);
    }
}
