package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public abstract class TransactionalRequest<T extends TransactionalRequest<T>>
        extends ru.yandex.yt.ytclient.request.TransactionalRequest.Builder<T> {
    TransactionalRequest() {
    }

    TransactionalRequest(TransactionalRequest<?> req) {
        super(req);
    }
}
