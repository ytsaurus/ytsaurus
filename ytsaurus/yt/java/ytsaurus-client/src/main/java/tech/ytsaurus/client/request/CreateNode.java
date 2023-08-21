package tech.ytsaurus.client.request;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TMutatingOptions;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqCreateNode;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ytree.TAttributeDictionary;

/**
 * Request for creating cypress node.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#create">
 * create documentation
 * </a>
 */
public class CreateNode
        extends MutatePath<CreateNode.Builder, CreateNode>
        implements HighLevelRequest<TReqCreateNode.Builder> {
    private final CypressNodeType type;
    private final boolean recursive;
    private final boolean force;
    private final boolean ignoreExisting;
    private final boolean lockExisting;
    private final Map<String, YTreeNode> attributes;

    public CreateNode(BuilderBase<?> builder) {
        super(builder);
        this.type = Objects.requireNonNull(builder.type);
        this.recursive = builder.recursive;
        this.force = builder.force;
        this.ignoreExisting = builder.ignoreExisting;
        this.lockExisting = builder.lockExisting;
        this.attributes = new HashMap<>(builder.attributes);
    }

    public CreateNode(YPath path, CypressNodeType type) {
        this(builder().setPath(path).setType(type));
    }

    public CreateNode(YPath path, CypressNodeType type, Map<String, YTreeNode> attributes) {
        this(builder().setPath(path).setType(type).setAttributes(attributes));
    }

    public static Builder builder() {
        return new Builder();
    }

    public CypressNodeType getType() {
        return type;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public boolean isForce() {
        return force;
    }

    public boolean isIgnoreExisting() {
        return ignoreExisting;
    }

    public boolean isLockExisting() {
        return lockExisting;
    }

    public Map<String, YTreeNode> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCreateNode.Builder, ?> builder) {
        builder.body()
                .setPath(path.toString())
                .setType(type.protoValue())
                .setRecursive(recursive)
                .setForce(force)
                .setIgnoreExisting(ignoreExisting)
                .setLockExisting(lockExisting);

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

        if (!attributes.isEmpty()) {
            final TAttributeDictionary.Builder aBuilder = builder.body().getAttributesBuilder();
            for (Map.Entry<String, YTreeNode> me : attributes.entrySet()) {
                aBuilder.addAttributesBuilder()
                        .setKey(me.getKey())
                        .setValue(ByteString.copyFrom(me.getValue().toBinary()));
            }
        }
    }

    public YTreeBuilder toTree(@Nonnull YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .key("type").value(type.value())
                .when(recursive, b -> b.key("recursive").value(recursive))
                .when(ignoreExisting, b -> b.key("ignore_existing").value(ignoreExisting))
                .when(lockExisting, b -> b.key("lock_existing").value(lockExisting))
                .when(force, b -> b.key("force").value(true))
                .when(!attributes.isEmpty(), b -> b.key("attributes").value(attributes));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("; Type:").append(type).append("; ");
        if (recursive) {
            sb.append("Recursive: true; ");
        }
        if (ignoreExisting) {
            sb.append("IgnoreExisting: true; ");
        }
        if (lockExisting) {
            sb.append("LockExisting: true; ");
        }
        if (force) {
            sb.append("Force: true; ");
        }
    }

    public Builder toBuilder() {
        return builder()
                .setType(type)
                .setForce(force)
                .setIgnoreExisting(ignoreExisting)
                .setRecursive(recursive)
                .setLockExisting(lockExisting)
                .setPath(path)
                .setAttributes(attributes)
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
            extends MutatePath.Builder<TBuilder, CreateNode> {
        @Nullable
        protected CypressNodeType type;
        protected boolean recursive = false;
        protected boolean force = false;
        protected boolean ignoreExisting = false;
        protected boolean lockExisting = false;
        protected final Map<String, YTreeNode> attributes = new HashMap<>();

        protected BuilderBase() {
        }

        public BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.type = builder.type;
            this.recursive = builder.recursive;
            this.force = builder.force;
            this.ignoreExisting = builder.ignoreExisting;
            this.lockExisting = builder.lockExisting;
            this.attributes.clear();
            this.attributes.putAll(builder.attributes);
        }

        public TBuilder setType(CypressNodeType type) {
            this.type = type;
            return self();
        }

        public TBuilder setRecursive(boolean recursive) {
            this.recursive = recursive;
            return self();
        }

        public TBuilder setForce(boolean force) {
            this.force = force;
            return self();
        }

        public TBuilder setIgnoreExisting(boolean ignoreExisting) {
            this.ignoreExisting = ignoreExisting;
            return self();
        }

        public TBuilder setLockExisting(boolean lockExisting) {
            this.lockExisting = lockExisting;
            return self();
        }

        public TBuilder addAttribute(String name, @Nullable Object value) {
            this.attributes.put(name, YTree.node(value));
            return self();
        }

        public TBuilder addAttribute(String name, YTreeNode value) {
            this.attributes.put(name, value);
            return self();
        }

        public TBuilder setAttributes(Map<String, YTreeNode> attributes) {
            this.attributes.clear();
            this.attributes.putAll(attributes);
            return self();
        }

        public TBuilder clearAttributes() {
            this.attributes.clear();
            return self();
        }

        public CypressNodeType getType() {
            return Objects.requireNonNull(type);
        }

        public boolean isRecursive() {
            return recursive;
        }

        public boolean isForce() {
            return force;
        }

        public boolean isIgnoreExisting() {
            return ignoreExisting;
        }

        public boolean isLockExisting() {
            return lockExisting;
        }

        public Map<String, YTreeNode> getAttributes() {
            return Collections.unmodifiableMap(attributes);
        }

        public YTreeBuilder toTree(@Nonnull YTreeBuilder builder) {
            Objects.requireNonNull(type);

            return builder
                    .apply(super::toTree)
                    .key("type").value(type.value())
                    .when(recursive, b -> b.key("recursive").value(recursive))
                    .when(ignoreExisting, b -> b.key("ignore_existing").value(ignoreExisting))
                    .when(lockExisting, b -> b.key("lock_existing").value(lockExisting))
                    .when(force, b -> b.key("force").value(true))
                    .when(!attributes.isEmpty(), b -> b.key("attributes").value(attributes));
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("; Type:").append(type).append("; ");
            if (recursive) {
                sb.append("Recursive: true; ");
            }
            if (ignoreExisting) {
                sb.append("IgnoreExisting: true; ");
            }
            if (lockExisting) {
                sb.append("LockExisting: true; ");
            }
            if (force) {
                sb.append("Force: true; ");
            }
        }

        @Override
        public CreateNode build() {
            return new CreateNode(this);
        }
    }
}
