package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;

@NonNullApi
public abstract class GetLikeReq<T extends GetLikeReq<T>> extends ru.yandex.yt.ytclient.request.GetLikeReq.Builder<T> {
    GetLikeReq() {
    }

    GetLikeReq(GetLikeReq<?> req) {
        super(req);
    }
}
