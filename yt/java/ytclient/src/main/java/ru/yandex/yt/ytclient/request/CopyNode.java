package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqCopyNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.PrerequisiteOptions;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class CopyNode
        extends CopyLikeReq<CopyNode.Builder, CopyNode>
        implements HighLevelRequest<TReqCopyNode.Builder> {
    public CopyNode(BuilderBase<?, ?> builder) {
        super(builder);
    }

    public CopyNode(String from, String to) {
        this(builder().setSource(from).setDestination(to));
    }

    public CopyNode(YPath from, YPath to) {
        this(from.justPath().toString(), to.justPath().toString());
    }

    public static Builder builder() {
        return new Builder();
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
        builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
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

    public static class Builder extends BuilderBase<Builder, CopyNode> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public CopyNode build() {
            return new CopyNode(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder, TRequest>,
            TRequest extends CopyLikeReq<?, TRequest>>
            extends CopyLikeReq.Builder<TBuilder, TRequest>
            implements HighLevelRequest<TReqCopyNode.Builder> {
        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?, ?> builder) {
            super(builder);
        }

        @Override
        public void writeTo(RpcClientRequestBuilder<TReqCopyNode.Builder, ?> requestBuilder) {
            TReqCopyNode.Builder builder = requestBuilder.body();
            builder.setSrcPath(Objects.requireNonNull(source))
                    .setDstPath(Objects.requireNonNull(destination))
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
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
            if (additionalData != null) {
                builder.mergeFrom(additionalData);
            }
        }
    }
}
