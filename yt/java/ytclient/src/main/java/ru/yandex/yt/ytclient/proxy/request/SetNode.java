package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class SetNode extends ru.yandex.yt.ytclient.request.SetNode.BuilderBase<SetNode> {
    public SetNode(ru.yandex.yt.ytclient.request.SetNode other) {
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
