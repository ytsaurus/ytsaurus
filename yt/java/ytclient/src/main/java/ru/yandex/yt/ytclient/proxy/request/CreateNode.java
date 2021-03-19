package ru.yandex.yt.ytclient.proxy.request;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;
import ru.yandex.inside.yt.kosher.cypress.YPath;
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
    private final int type;

    private boolean recursive = false;
    private boolean force = false;
    private boolean ignoreExisting = false;
    private final Map<String, YTreeNode> attributes = new HashMap<>();

    public CreateNode(String path, ObjectType type) {
        super(YPath.simple(path));
        this.type = type.value();
    }

    public CreateNode(YPath path, ObjectType type) {
        super(path);
        this.type = type.value();
    }

    public CreateNode(String path, ObjectType type, Map<String, YTreeNode> attributes) {
        this(path, type);
        setAttributes(attributes);
    }

    public CreateNode(YPath path, CypressNodeType type, Map<String, YTreeNode> attributes) {
        this(path, ObjectType.from(type));
        setAttributes(attributes);
    }

    public CreateNode setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    public CreateNode setForce(boolean force) {
        this.force = force;
        return this;
    }

    public CreateNode setIgnoreExisting(boolean ignoreExisting) {
        this.ignoreExisting = ignoreExisting;
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
                .setType(type)
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
