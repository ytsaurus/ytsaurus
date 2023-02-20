package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class RemoveNode extends tech.ytsaurus.client.request.RemoveNode.BuilderBase<RemoveNode> {
    public RemoveNode(tech.ytsaurus.client.request.RemoveNode other) {
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
