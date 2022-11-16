package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class MoveNode extends tech.ytsaurus.client.request.MoveNode.BuilderBase<MoveNode> {
    public MoveNode(String src, String dst) {
        setSource(src).setDestination(dst);
    }

    public MoveNode(YPath src, YPath dst) {
        this(src.justPath().toString(), dst.justPath().toString());
    }

    public MoveNode(tech.ytsaurus.client.request.MoveNode.BuilderBase<?> builder) {
        super(builder);
    }

    @Override
    protected MoveNode self() {
        return this;
    }
}
