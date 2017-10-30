package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqCreateNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class CreateNode extends MutateNode<CreateNode> {
    private final String path;
    private final int type;

    // TODO: AttributeDictionary

    private boolean recursive = false;
    private boolean force = false;
    private boolean ignoreExisting = false;

    public CreateNode(String path, ObjectType type) {
        this.path = path;
        this.type = type.value();
    }

    public CreateNode setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    public CreateNode setForce(boolean force) {
        this.force = force;
        return this;
    }

    public CreateNode setIgnoreExisting(boolean ignoreExisting) {
        this.ignoreExisting = ignoreExisting;
        return this;
    }

    public TReqCreateNode.Builder writeTo(TReqCreateNode.Builder builder) {
        builder.setPath(path)
                .setType(type)
                .setRecursive(recursive)
                .setForce(force)
                .setIgnoreExisting(ignoreExisting);

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
}
