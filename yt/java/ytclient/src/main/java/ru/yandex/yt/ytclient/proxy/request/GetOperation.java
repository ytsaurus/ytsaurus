package ru.yandex.yt.ytclient.proxy.request;


import tech.ytsaurus.core.GUID;
public class GetOperation extends ru.yandex.yt.ytclient.request.GetOperation.BuilderBase<GetOperation> {
    public GetOperation(GUID guid) {
        setOperationId(guid);
    }

    @Override
    protected GetOperation self() {
        return this;
    }
}
