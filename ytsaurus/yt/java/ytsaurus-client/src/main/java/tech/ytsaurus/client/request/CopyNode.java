package tech.ytsaurus.client.request;

import java.util.Objects;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TMutatingOptions;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqCopyNode;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;

public class CopyNode
        extends CopyLikeReq<CopyNode.Builder, CopyNode>
        implements HighLevelRequest<TReqCopyNode.Builder> {
    public CopyNode(BuilderBase<?> builder) {
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

    /**
     * Internal method: prepare request to send over network.
     */
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

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends CopyLikeReq.Builder<TBuilder, CopyNode>
            implements HighLevelRequest<TReqCopyNode.Builder> {
        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
        }

        /**
         * Internal method: prepare request to send over network.
         */
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

        @Override
        public CopyNode build() {
            return new CopyNode(this);
        }
    }
}
