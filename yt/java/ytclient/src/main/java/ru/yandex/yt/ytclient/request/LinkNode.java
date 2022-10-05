package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqLinkNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.PrerequisiteOptions;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class LinkNode
        extends CopyLikeReq<LinkNode.Builder, LinkNode>
        implements HighLevelRequest<TReqLinkNode.Builder> {
    public LinkNode(BuilderBase<?> builder) {
        super(builder);
    }

    public LinkNode(String src, String dst) {
        this(builder().setSource(src).setDestination(dst));
    }

    public LinkNode(YPath src, YPath dst) {
        this(src.justPath().toString(), dst.justPath().toString());
    }

    public static Builder builder() {
        return new Builder();
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
        builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return super.toTree(builder, "target_path", "link_path");
    }

    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setSource(source)
                .setDestination(destination)
                .setRecursive(recursive)
                .setForce(force)
                .setPreserveAccount(preserveAccount)
                .setPreserveExpirationTime(preserveExpirationTime)
                .setPreserveCreationTime(preserveCreationTime)
                .setIgnoreExisting(ignoreExisting)
                .setTransactionalOptions(transactionalOptions != null
                        ? new TransactionalOptions(transactionalOptions)
                        : null)
                .setPrerequisiteOptions(prerequisiteOptions != null
                        ? new PrerequisiteOptions(prerequisiteOptions)
                        : null)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);

        builder.setMutatingOptions(new MutatingOptions(mutatingOptions));
        return builder;
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends CopyLikeReq.Builder<TBuilder, LinkNode> {
        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
        }

        @Override
        public YTreeBuilder toTree(YTreeBuilder builder) {
            return super.toTree(builder, "target_path", "link_path");
        }

        @Override
        public LinkNode build() {
            return new LinkNode(this);
        }
    }
}
