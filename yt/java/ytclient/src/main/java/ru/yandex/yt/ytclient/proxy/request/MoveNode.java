package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class MoveNode extends ru.yandex.yt.ytclient.request.MoveNode.BuilderBase<
        MoveNode, ru.yandex.yt.ytclient.request.MoveNode> {
    public MoveNode(String src, String dst) {
        setSource(src).setDestination(dst);
    }

    public MoveNode(YPath src, YPath dst) {
        this(src.justPath().toString(), dst.justPath().toString());
    }

    public MoveNode(ru.yandex.yt.ytclient.request.MoveNode.BuilderBase<?, ?> builder) {
        super(builder);
    }

    @Override
    protected MoveNode self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.MoveNode build() {
        return new ru.yandex.yt.ytclient.request.MoveNode(this);
    }
}
