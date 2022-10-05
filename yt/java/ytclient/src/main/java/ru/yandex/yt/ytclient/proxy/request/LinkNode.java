package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class LinkNode extends ru.yandex.yt.ytclient.request.LinkNode.BuilderBase<LinkNode> {
    public LinkNode(String src, String dst) {
        setSource(src).setDestination(dst);
    }

    public LinkNode(YPath src, YPath dst) {
        this(src.justPath().toString(), dst.justPath().toString());
    }

    public LinkNode(ru.yandex.yt.ytclient.request.LinkNode.BuilderBase<?> linkNode) {
        super(linkNode);
    }

    @Override
    protected LinkNode self() {
        return this;
    }
}
