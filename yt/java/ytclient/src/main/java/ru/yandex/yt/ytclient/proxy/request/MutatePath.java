package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public abstract class MutatePath<T extends MutatePath<T>> extends MutateNode<T> {
    final YPath path;

    MutatePath(YPath path) {
        this.path = path.justPath();
    }

    MutatePath(MutatePath<T> other) {
        super(other);
        path = other.path;
    }

    public YPath getPath() {
        return path;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("Path: ").append(path).append("; ");
        super.writeArgumentsLogString(sb);
    }
}
