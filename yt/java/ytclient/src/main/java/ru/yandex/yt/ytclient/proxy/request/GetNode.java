package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;

public class GetNode extends ru.yandex.yt.ytclient.request.GetNode.BuilderBase<
        GetNode, ru.yandex.yt.ytclient.request.GetNode> {
    public GetNode(String path) {
        this(YPath.simple(path));
    }

    public GetNode() {
    }

    public GetNode(YPath path) {
        setPath(path);
    }

    public GetNode(GetNode getNode) {
        super(getNode);
    }

    public GetNode(ru.yandex.yt.ytclient.request.GetNode getNode) {
        super(getNode.toBuilder());
    }

    @Override
    protected GetNode self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.GetNode build() {
        return new ru.yandex.yt.ytclient.request.GetNode(this);
    }
}
