package tech.ytsaurus.client.request;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.rpcproxy.TReqCreateObject;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ytree.TAttributeDictionary;

public class CreateObject extends RequestBase<CreateObject.Builder, CreateObject>
        implements HighLevelRequest<TReqCreateObject.Builder> {
    private final CypressNodeType type;
    private final Map<String, YTreeNode> attributes;
    private final boolean ignoreExisting;

    CreateObject(Builder builder) {
        super(builder);
        this.type = Objects.requireNonNull(builder.type);
        this.attributes = new HashMap<>(builder.attributes);
        this.ignoreExisting = builder.ignoreExisting;
    }

    public CreateObject(CypressNodeType type) {
        this(builder().setType(type));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCreateObject.Builder, ?> builder) {
        builder.body().setType(type.protoValue());
        builder.body().setIgnoreExisting(ignoreExisting);

        final TAttributeDictionary.Builder aBuilder = builder.body().getAttributesBuilder();
        for (Map.Entry<String, YTreeNode> me : attributes.entrySet()) {
            aBuilder.addAttributesBuilder()
                    .setKey(me.getKey())
                    .setValue(ByteString.copyFrom(me.getValue().toBinary()));
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Type: ").append(type).append("; IgnoreExisting: ").append(ignoreExisting).append("; ");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setType(type)
                .setAttributes(attributes)
                .setIgnoreExisting(ignoreExisting)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, CreateObject> {
        @Nullable
        private CypressNodeType type;
        private Map<String, YTreeNode> attributes = new HashMap<>();
        private boolean ignoreExisting = TReqCreateObject.getDefaultInstance().getIgnoreExisting();

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.type = builder.type;
            this.attributes = new HashMap<>(builder.attributes);
            this.ignoreExisting = builder.ignoreExisting;
        }

        public Builder setType(CypressNodeType type) {
            this.type = type;
            return self();
        }

        public Builder addAttribute(String name, YTreeNode value) {
            this.attributes.put(name, value);
            return self();
        }

        public Builder setAttributes(Map<String, YTreeNode> attributes) {
            this.attributes.clear();
            this.attributes.putAll(attributes);
            return self();
        }

        public Builder setIgnoreExisting(boolean ignoreExisting) {
            this.ignoreExisting = ignoreExisting;
            return self();
        }

        public CreateObject build() {
            return new CreateObject(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
