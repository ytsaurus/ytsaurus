package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqLinkNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class LinkNode extends CopyLikeReq<LinkNode> implements HighLevelRequest<TReqLinkNode.Builder> {
    public LinkNode(String src, String dst) {
        super(src, dst);
    }

    public LinkNode(YPath src, YPath dst) {
        this(src.justPath().toString(), dst.justPath().toString());
    }

    public LinkNode(LinkNode linkNode) {
        super(linkNode);
    }


    @Override
    public void writeTo(RpcClientRequestBuilder<TReqLinkNode.Builder, ?> requestBuilder) {
        TReqLinkNode.Builder builder = requestBuilder.body();
        builder.setSrcPath(source)
                .setDstPath(destination)
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
    }

    @Nonnull
    @Override
    protected LinkNode self() {
        return this;
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return super.toTree(builder, "target_path", "link_path");
    }
}
