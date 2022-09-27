package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;

public class CopyNode extends ru.yandex.yt.ytclient.request.CopyNode.BuilderBase<
        CopyNode, ru.yandex.yt.ytclient.request.CopyNode> {
    public CopyNode(String from, String to) {
        setSource(from).setDestination(to);
    }

    public CopyNode(YPath from, YPath to) {
        this(from.justPath().toString(), to.justPath().toString());
    }

    public CopyNode(ru.yandex.yt.ytclient.request.CopyNode.BuilderBase<?, ?> copyNode) {
        super(copyNode);
    }

    @Override
    protected CopyNode self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.CopyNode build() {
        return new ru.yandex.yt.ytclient.request.CopyNode(this);
    }
}
