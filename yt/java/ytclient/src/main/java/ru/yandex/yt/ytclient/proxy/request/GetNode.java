package ru.yandex.yt.ytclient.proxy.request;


import tech.ytsaurus.core.cypress.YPath;
public class GetNode extends ru.yandex.yt.ytclient.request.GetNode.BuilderBase<GetNode> {
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
}
