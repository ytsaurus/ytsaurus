package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TMutatingOptions;
import tech.ytsaurus.rpcproxy.TReqRemoveNode;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTreeBuilder;

public class RemoveNode
        extends MutatePath<RemoveNode.Builder, RemoveNode>
        implements HighLevelRequest<TReqRemoveNode.Builder> {
    private final boolean recursive;
    private final boolean force;

    public RemoveNode(BuilderBase<?> builder) {
        super(builder);
        this.recursive = builder.recursive;
        this.force = builder.force;
    }

    public RemoveNode(YPath path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isRecursive() {
        return recursive;
    }

    public boolean isForce() {
        return force;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqRemoveNode.Builder, ?> builder) {
        builder.body()
                .setPath(path.toString())
                .setRecursive(recursive)
                .setForce(force);

        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        builder.body().setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
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

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setForce(force)
                .setRecursive(recursive)
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

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends MutatePath.Builder<TBuilder, RemoveNode> {
        private boolean recursive = true;
        private boolean force = false;

        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.recursive = builder.recursive;
            this.force = builder.force;
        }

        public TBuilder setRecursive(boolean recursive) {
            this.recursive = recursive;
            return self();
        }

        public TBuilder setForce(boolean force) {
            this.force = force;
            return self();
        }

        public boolean isRecursive() {
            return recursive;
        }

        public boolean isForce() {
            return force;
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

        @Override
        public RemoveNode build() {
            return new RemoveNode(this);
        }
    }
}
