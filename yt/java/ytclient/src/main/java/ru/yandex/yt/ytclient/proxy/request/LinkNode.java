package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class LinkNode extends tech.ytsaurus.client.request.LinkNode.BuilderBase<LinkNode> {
    public LinkNode(String src, String dst) {
        setSource(src).setDestination(dst);
    }

    public LinkNode(YPath src, YPath dst) {
        this(src.justPath().toString(), dst.justPath().toString());
    }

    public LinkNode(tech.ytsaurus.client.request.LinkNode.BuilderBase<?> linkNode) {
        super(linkNode);
    }

    @Override
    protected LinkNode self() {
        return this;
    }
}
