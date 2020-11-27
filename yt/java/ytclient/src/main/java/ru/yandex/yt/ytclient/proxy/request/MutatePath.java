package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public abstract class MutatePath<T extends MutatePath<T>> extends MutateNode<T> {
    final String path;

    MutatePath(String path) {
        this.path = path;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(path).append("; ");
        super.writeArgumentsLogString(sb);
    }
}
