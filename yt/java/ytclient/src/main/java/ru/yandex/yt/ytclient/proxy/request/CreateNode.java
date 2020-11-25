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
import ru.yandex.yt.ytree.TAttributeDictionary;

public class CreateNode extends MutateNode<CreateNode> {
    private final String path;
    private final int type;

    private boolean recursive = false;
    private boolean force = false;
    private boolean ignoreExisting = false;
    private Map<String, YTreeNode> attributes = new HashMap<>();

    public CreateNode(String path, ObjectType type) {
        this.path = path;
        this.type = type.value();
    }

    public CreateNode(YPath path, ObjectType type) {
        this.path = path.toString();
        this.type = type.value();
    }

    public CreateNode(String path, ObjectType type, Map<String, YTreeNode> attributes) {
        this(path, type);
        setAttributes(attributes);
    }

    public CreateNode(YPath path, CypressNodeType type, Map<String, YTreeNode> attributes) {
        this(path.toString(), ObjectType.from(type));
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

    public TReqCreateNode.Builder writeTo(TReqCreateNode.Builder builder) {
        builder.setPath(path)
                .setType(type)
                .setRecursive(recursive)
                .setForce(force)
                .setIgnoreExisting(ignoreExisting);

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }

        if (!attributes.isEmpty()) {
            final TAttributeDictionary.Builder aBuilder = builder.getAttributesBuilder();
            for (Map.Entry<String, YTreeNode> me : attributes.entrySet()) {
                aBuilder.addAttributesBuilder()
                        .setKey(me.getKey())
                        .setValue(ByteString.copyFrom(me.getValue().toBinary()));
            }
        }
        return builder;
    }

    @Nonnull
    @Override
    protected CreateNode self() {
        return this;
    }
}
