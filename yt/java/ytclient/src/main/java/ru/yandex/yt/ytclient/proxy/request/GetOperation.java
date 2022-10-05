package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;

public class GetOperation extends ru.yandex.yt.ytclient.request.GetOperation.BuilderBase<GetOperation> {
    public GetOperation(GUID guid) {
        setId(guid);
    }

    @Override
    protected GetOperation self() {
        return this;
    }
}
