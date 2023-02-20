package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
public class ExistsNode extends tech.ytsaurus.client.request.ExistsNode.BuilderBase<ExistsNode> {
    public ExistsNode(String path) {
        this(YPath.simple(path));
    }

    public ExistsNode() {
    }

    public ExistsNode(YPath path) {
        setPath(path);
    }

    public ExistsNode(ExistsNode existsNode) {
        super(existsNode);
    }

    public ExistsNode(tech.ytsaurus.client.request.ExistsNode existsNode) {
        super(existsNode.toBuilder());
    }

    @Override
    protected ExistsNode self() {
        return this;
    }
}
