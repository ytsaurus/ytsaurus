package ru.yandex.yt.ytclient.proxy.request;


import tech.ytsaurus.core.cypress.YPath;
public class CopyNode extends ru.yandex.yt.ytclient.request.CopyNode.BuilderBase<CopyNode> {
    public CopyNode(String from, String to) {
        setSource(from).setDestination(to);
    }

    public CopyNode(YPath from, YPath to) {
        this(from.justPath().toString(), to.justPath().toString());
    }

    public CopyNode(ru.yandex.yt.ytclient.request.CopyNode.BuilderBase<?> copyNode) {
        super(copyNode);
    }

    @Override
    protected CopyNode self() {
        return this;
    }
}
