package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqRemoveNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class RemoveNode extends MutatePath<RemoveNode> implements HighLevelRequest<TReqRemoveNode.Builder> {
    private boolean recursive = true;
    private boolean force = false;

    public RemoveNode(String path) {
        super(YPath.simple(path));
    }

    public RemoveNode(YPath path) {
        super(path);
    }

    public RemoveNode(RemoveNode other) {
        super(other);
        this.recursive = other.recursive;
        this.force = other.force;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public RemoveNode setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public RemoveNode setForce(boolean force) {
        this.force = force;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqRemoveNode.Builder, ?> builder) {
        builder.body()
                .setPath(path.toString())
                .setRecursive(recursive)
                .setForce(force);

        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.body().setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.body().mergeFrom(additionalData);
        }
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .when(recursive, b -> b.key("recursive").value(recursive))
                .when(force, b -> b.key("force").value(true));
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        if (force) {
            sb.append("Force: true; ");
        }
        if (recursive) {
            sb.append("Recursive: true; ");
        }
    }

    @Nonnull
    @Override
    protected RemoveNode self() {
        return this;
    }
}
