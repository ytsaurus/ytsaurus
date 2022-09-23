package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public abstract class MutateNode<T extends MutateNode<T>> extends ru.yandex.yt.ytclient.request.MutateNode.Builder<T> {
    protected MutateNode() {
    }

    protected MutateNode(MutateNode<?> other) {
        super(other);
    }
}
