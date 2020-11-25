package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqLockNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class LockNode extends MutateNode<LockNode> {
    private final String path;
    private final int mode;

    private boolean waitable = false;
    private String childKey;
    private String attributeKey;

    public LockNode(String path, LockMode mode) {
        this.path = path;
        this.mode = mode.value();
    }

    public LockNode(YPath path, LockMode mode) {
        this.path = path.toString();
        this.mode = mode.value();
    }

    public LockNode setWaitable(boolean waitable) {
        this.waitable = waitable;
        return this;
    }

    public LockNode setChildKey(String childKey) {
        this.childKey = childKey;
        return this;
    }

    public LockNode setAttributeKey(String attributeKey) {
        this.attributeKey = attributeKey;
        return this;
    }

    public TReqLockNode.Builder writeTo(TReqLockNode.Builder builder) {
        builder.setPath(path)
                .setMode(mode)
                .setWaitable(waitable);

        if (childKey != null) {
            builder.setChildKey(childKey);
        }
        if (attributeKey != null) {
            builder.setAttributeKey(attributeKey);
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }

    @Nonnull
    @Override
    protected LockNode self() {
        return this;
    }
}
