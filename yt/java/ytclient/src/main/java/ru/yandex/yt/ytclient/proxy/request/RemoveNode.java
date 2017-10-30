package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqRemoveNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class RemoveNode extends MutateNode<RemoveNode> {
    private final String path;
    private boolean recursive = true;
    private boolean force = false;

    public RemoveNode(String path) {
        this.path = path;
    }

    public RemoveNode setRecursive(boolean f) {
        this.recursive = f;
        return this;
    }

    public RemoveNode setForce(boolean f) {
        this.force = f;
        return this;
    }

    public TReqRemoveNode.Builder writeTo(TReqRemoveNode.Builder builder) {
        builder.setPath(path)
                .setRecursive(recursive)
                .setForce(force);

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }

        return builder;
    }
}
