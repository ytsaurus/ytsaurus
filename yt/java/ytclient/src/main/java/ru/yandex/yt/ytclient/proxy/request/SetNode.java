package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeNode;


@NonNullApi
@NonNullFields
public class SetNode extends tech.ytsaurus.client.request.SetNode.BuilderBase<SetNode> {
    public SetNode(tech.ytsaurus.client.request.SetNode other) {
        super(other.toBuilder());
    }

    public SetNode(SetNode other) {
        super(other);
    }

    public SetNode(String path, byte[] value) {
        setPath(YPath.simple(path)).setValue(value).setForce(false);
    }

    public SetNode(YPath path, YTreeNode value) {
        setPath(path).setValue(value);
    }

    @Override
    protected SetNode self() {
        return this;
    }
}
