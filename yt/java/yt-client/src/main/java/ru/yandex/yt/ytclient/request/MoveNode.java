package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqMoveNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.PrerequisiteOptions;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class MoveNode
        extends CopyLikeReq<MoveNode.Builder, MoveNode>
        implements HighLevelRequest<TReqMoveNode.Builder> {
    public MoveNode(BuilderBase<?> builder) {
        super(builder);
    }

    public MoveNode(String src, String dst) {
        this(builder().setSource(src).setDestination(dst));
    }

    public MoveNode(YPath src, YPath dst) {
        this(src.justPath().toString(), dst.justPath().toString());
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqMoveNode.Builder, ?> requestBuilder) {
        TReqMoveNode.Builder builder = requestBuilder.body();
        builder.setSrcPath(source)
                .setDstPath(destination)
                .setRecursive(recursive)
                .setForce(force)
                .setPreserveAccount(preserveAccount)
                .setPreserveExpirationTime(preserveExpirationTime);

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
        return builder()
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
                .setAdditionalData(additionalData)
                .setMutatingOptions(new MutatingOptions(mutatingOptions));
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends CopyLikeReq.Builder<TBuilder, MoveNode> {
        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
        }

        @Override
        public MoveNode build() {
            return new MoveNode(this);
        }
    }
}
