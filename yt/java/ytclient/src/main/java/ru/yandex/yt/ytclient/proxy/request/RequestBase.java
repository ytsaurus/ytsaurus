package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public abstract class RequestBase<T extends RequestBase<T>>
        extends ru.yandex.yt.ytclient.request.RequestBase.Builder<T> {
    public RequestBase() {
    }

    protected RequestBase(RequestBase<?> req) {
        super(req);
    }
}
