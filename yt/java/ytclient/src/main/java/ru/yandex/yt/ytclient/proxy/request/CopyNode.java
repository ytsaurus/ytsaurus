package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqCopyNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class CopyNode extends CopyLikeReq<CopyNode> implements HighLevelRequest<TReqCopyNode.Builder> {
    public CopyNode(String from, String to) {
        super(from, to);
    }

    public CopyNode(YPath from, YPath to) {
        this(from.justPath().toString(), to.justPath().toString());
    }

    public CopyNode(CopyNode copyNode) {
        super(copyNode);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCopyNode.Builder, ?> requestBuilder) {
        TReqCopyNode.Builder builder = requestBuilder.body();
        builder.setSrcPath(source)
                .setDstPath(destination)
                .setRecursive(recursive)
                .setForce(force)
                .setPreserveAccount(preserveAccount)
                .setPreserveExpirationTime(preserveExpirationTime)
                .setPreserveCreationTime(preserveCreationTime);

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
    }

    @Nonnull
    @Override
    protected CopyNode self() {
        return this;
    }
}
