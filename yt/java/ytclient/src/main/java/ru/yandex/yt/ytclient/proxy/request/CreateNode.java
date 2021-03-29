package ru.yandex.yt.ytclient.proxy.request;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqCreateNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytree.TAttributeDictionary;

/**
 * Request for creating cypress node.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#create">
 *     create documentation
 *     </a>
 */
public class CreateNode extends MutatePath<CreateNode> implements HighLevelRequest<TReqCreateNode.Builder> {
    private final ObjectType type;

    private boolean recursive = false;
    private boolean force = false;
    private boolean ignoreExisting = false;
    private final Map<String, YTreeNode> attributes = new HashMap<>();

    public CreateNode(CreateNode other) {
        super(other);
        type = other.type;
        recursive = other.recursive;
        force = other.force;
        ignoreExisting = other.ignoreExisting;
        attributes.putAll(other.attributes);
    }

    public CreateNode(String path, ObjectType type) {
        super(YPath.simple(path));
        this.type = type;
    }

    public CreateNode(YPath path, ObjectType type) {
        super(path);
        this.type = type;
    }

    public CreateNode(String path, ObjectType type, Map<String, YTreeNode> attributes) {
        this(path, type);
        setAttributes(attributes);
    }

    public CreateNode(YPath path, CypressNodeType type, Map<String, YTreeNode> attributes) {
        this(path, ObjectType.from(type));
        setAttributes(attributes);
    }

    public ObjectType getType() {
        return type;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public CreateNode setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public CreateNode setForce(boolean force) {
        this.force = force;
        return this;
    }

    public boolean isIgnoreExisting() {
        return ignoreExisting;
    }

    public CreateNode setIgnoreExisting(boolean ignoreExisting) {
        this.ignoreExisting = ignoreExisting;
        return this;
    }

    public Map<String, YTreeNode> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    public CreateNode addAttribute(String name, @Nullable Object value) {
        this.attributes.put(name, YTree.node(value));
        return this;
    }

    public CreateNode addAttribute(String name, YTreeNode value) {
        this.attributes.put(name, value);
        return this;
    }

    public CreateNode setAttributes(Map<String, YTreeNode> attributes) {
        this.attributes.clear();
        this.attributes.putAll(attributes);
        return this;
    }

    public CreateNode clearAttributes() {
        this.attributes.clear();
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCreateNode.Builder, ?> builder) {
        builder.body()
                .setPath(path.toString())
                .setType(type.value())
                .setRecursive(recursive)
                .setForce(force)
                .setIgnoreExisting(ignoreExisting);

        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.body().setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.body().setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
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
                .key("type").value(type.toCypressNodeType().value())
                .when(recursive, b -> b.key("recursive").value(recursive))
                .when(ignoreExisting, b -> b.key("ignore_existing").value(ignoreExisting))
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
        if (force) {
            sb.append("Force: true; ");
        }
    }

    @Nonnull
    @Override
    protected CreateNode self() {
        return this;
    }
}
