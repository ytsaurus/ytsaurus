package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class RemoveNode extends ru.yandex.yt.ytclient.request.RemoveNode.BuilderBase<RemoveNode> {
    public RemoveNode(ru.yandex.yt.ytclient.request.RemoveNode other) {
        super(other.toBuilder());
    }

    public RemoveNode(String path) {
        setPath(YPath.simple(path));
    }

    public RemoveNode(YPath path) {
        setPath(path);
    }

    public RemoveNode(RemoveNode other) {
        super(other);
    }

    @Override
    protected RemoveNode self() {
        return this;
    }
}
