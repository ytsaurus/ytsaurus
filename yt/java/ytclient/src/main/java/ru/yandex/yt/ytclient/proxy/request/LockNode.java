package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.request.LockMode;

@NonNullFields
@NonNullApi
public class LockNode extends ru.yandex.yt.ytclient.request.LockNode.BuilderBase<LockNode> {
    public LockNode(LockNode other) {
        super(other);
    }

    public LockNode(String path, LockMode mode) {
        setPath(YPath.simple(path)).setMode(mode);
    }

    public LockNode(YPath path, LockMode mode) {
        setPath(path).setMode(mode);
    }

    @Override
    protected LockNode self() {
        return this;
    }
}
