package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TMutatingOptions;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqSetNode;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

public class SetNode extends MutatePath<SetNode.Builder, SetNode> implements HighLevelRequest<TReqSetNode.Builder> {
    private final byte[] value;
    private final boolean force;
    private final boolean recursive;

    public SetNode(BuilderBase<?> builder) {
        super(builder);
        this.value = Objects.requireNonNull(builder.value).clone();
        this.force = builder.force;
        this.recursive = builder.recursive;
    }

    public SetNode(YPath path, YTreeNode value) {
        this(builder().setPath(path).setValue(value));
    }

    public static Builder builder() {
        return new Builder();
    }

    public byte[] getValue() {
        return value;
    }

    public boolean isForce() {
        return force;
    }

    public boolean isRecursive() {
        return recursive;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqSetNode.Builder, ?> builder) {
        builder.body().setPath(path.toString())
                .setForce(force)
                .setValue(ByteString.copyFrom(value));
        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.body().setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        builder.body().setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        if (additionalData != null) {
            builder.body().mergeFrom(additionalData);
        }
        if (recursive) {
            builder.body().setRecursive(true);
        }
    }

    @Override
    public YTreeBuilder toTree(@Nonnull YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .when(force, b -> b.key("force").value(true))
                .when(recursive, b -> b.key("recursive").value(true));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
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
                .setValue(value)
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
            extends MutatePath.Builder<TBuilder, SetNode> {
        @Nullable
        private byte[] value;
        private boolean force;
        private boolean recursive;

        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
            if (builder.value != null) {
                this.value = Arrays.copyOf(builder.value, builder.value.length);
            }
            this.force = builder.force;
            this.recursive = builder.recursive;
        }

        public TBuilder setForce(boolean force) {
            this.force = force;
            return self();
        }

        public TBuilder setRecursive(boolean recursive) {
            this.recursive = recursive;
            return self();
        }

        public TBuilder setValue(byte[] value) {
            this.value = Arrays.copyOf(value, value.length);
            return self();
        }

        public TBuilder setValue(YTreeNode value) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                YTreeBinarySerializer.serialize(value, baos);

                this.value = baos.toByteArray();

                baos.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return self();
        }

        public byte[] getValue() {
            return Objects.requireNonNull(value);
        }

        public boolean isForce() {
            return force;
        }

        public boolean isRecursive() {
            return recursive;
        }

        @Override
        public YTreeBuilder toTree(YTreeBuilder builder) {
            return builder
                    .apply(super::toTree)
                    .when(force, b -> b.key("force").value(true))
                    .when(recursive, b -> b.key("recursive").value(true));
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
        public SetNode build() {
            return new SetNode(this);
        }
    }
}
