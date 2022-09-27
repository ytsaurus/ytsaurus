package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;

@NonNullApi
public class ExistsNode extends ru.yandex.yt.ytclient.request.ExistsNode.BuilderBase<
        ExistsNode, ru.yandex.yt.ytclient.request.ExistsNode> {
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

    public ExistsNode(ru.yandex.yt.ytclient.request.ExistsNode existsNode) {
        super(existsNode.toBuilder());
    }

    @Override
    protected ExistsNode self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.ExistsNode build() {
        return new ru.yandex.yt.ytclient.request.ExistsNode(this);
    }
}
