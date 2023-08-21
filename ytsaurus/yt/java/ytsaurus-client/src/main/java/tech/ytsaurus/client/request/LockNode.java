package tech.ytsaurus.client.request;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.request.LockMode;
import tech.ytsaurus.rpcproxy.TMutatingOptions;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqLockNode;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTreeBuilder;

public class LockNode extends MutatePath<LockNode.Builder, LockNode> implements HighLevelRequest<TReqLockNode.Builder> {
    private final LockMode mode;

    private final boolean waitable;
    @Nullable
    private final String childKey;
    @Nullable
    private final String attributeKey;

    public LockNode(BuilderBase<?> builder) {
        super(builder);
        this.mode = Objects.requireNonNull(builder.mode);
        this.waitable = builder.waitable;
        this.childKey = builder.childKey;
        this.attributeKey = builder.attributeKey;
    }

    public LockNode(YPath path, LockMode mode) {
        this(builder().setPath(path).setMode(mode));
    }

    public static Builder builder() {
        return new Builder();
    }

    public LockMode getMode() {
        return mode;
    }

    public boolean isWaitable() {
        return waitable;
    }

    public Optional<String> getChildKey() {
        return Optional.ofNullable(childKey);
    }

    public Optional<String> getAttributeKey() {
        return Optional.ofNullable(attributeKey);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqLockNode.Builder, ?> builder) {
        builder.body()
                .setPath(path.toString())
                .setMode(mode.getProtoValue())
                .setWaitable(waitable);

        if (childKey != null) {
            builder.body().setChildKey(childKey);
        }
        if (attributeKey != null) {
            builder.body().setAttributeKey(attributeKey);
        }
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
    }

    public YTreeBuilder toTree(@Nonnull YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .key("mode").value(mode.getWireName())
                .when(waitable, b -> b.key("waitable").value(true))
                .when(childKey != null, b -> b.key("child_key").value(childKey))
                .when(attributeKey != null, b -> b.key("attribute_key").value(attributeKey));
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Mode: ").append(mode).append("; ");
        if (waitable) {
            sb.append("Waitable: true; ");
        }
        if (childKey != null) {
            sb.append("ChildKey: ").append(childKey).append("; ");
        }
        if (attributeKey != null) {
            sb.append("AttributeKey: ").append(attributeKey).append("; ");
        }
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setMode(mode)
                .setWaitable(waitable)
                .setChildKey(childKey)
                .setAttributeKey(attributeKey)
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
            extends MutatePath.Builder<TBuilder, LockNode> {
        @Nullable
        private LockMode mode;

        private boolean waitable = false;
        @Nullable
        private String childKey;
        @Nullable
        private String attributeKey;

        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.mode = builder.mode;
            this.waitable = builder.waitable;
            this.childKey = builder.childKey;
            this.attributeKey = builder.attributeKey;
        }

        public TBuilder setMode(LockMode mode) {
            this.mode = mode;
            return self();
        }

        public TBuilder setWaitable(boolean waitable) {
            this.waitable = waitable;
            return self();
        }

        public TBuilder setChildKey(@Nullable String childKey) {
            this.childKey = childKey;
            return self();
        }

        public TBuilder setAttributeKey(@Nullable String attributeKey) {
            this.attributeKey = attributeKey;
            return self();
        }

        public YTreeBuilder toTree(@Nonnull YTreeBuilder builder) {
            return builder
                    .apply(super::toTree)
                    .key("mode").value(Objects.requireNonNull(mode).getWireName())
                    .when(waitable, b -> b.key("waitable").value(true))
                    .when(childKey != null, b -> b.key("child_key").value(childKey))
                    .when(attributeKey != null, b -> b.key("attribute_key").value(attributeKey));
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("Mode: ").append(mode).append("; ");
            if (waitable) {
                sb.append("Waitable: true; ");
            }
            if (childKey != null) {
                sb.append("ChildKey: ").append(childKey).append("; ");
            }
            if (attributeKey != null) {
                sb.append("AttributeKey: ").append(attributeKey).append("; ");
            }
        }

        public LockMode getMode() {
            return Objects.requireNonNull(mode);
        }

        public boolean isWaitable() {
            return waitable;
        }

        public Optional<String> getChildKey() {
            return Optional.ofNullable(childKey);
        }

        public Optional<String> getAttributeKey() {
            return Optional.ofNullable(attributeKey);
        }

        @Override
        public LockNode build() {
            return new LockNode(this);
        }
    }
}
