package ru.yandex.yt.ytclient.request;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqCreateObject;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytree.TAttributeDictionary;

@NonNullApi
@NonNullFields
public class CreateObject extends RequestBase<CreateObject.Builder, CreateObject>
        implements HighLevelRequest<TReqCreateObject.Builder> {
    private final ObjectType type;
    private final Map<String, YTreeNode> attributes;
    private final boolean ignoreExisting;

    CreateObject(Builder builder) {
        super(builder);
        this.type = Objects.requireNonNull(builder.type);
        this.attributes = new HashMap<>(builder.attributes);
        this.ignoreExisting = builder.ignoreExisting;
    }

    public CreateObject(ObjectType type) {
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
        builder.body().setType(type.value());
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

    @NonNullApi
    @NonNullFields
    public static class Builder extends RequestBase.Builder<Builder, CreateObject> {
        @Nullable
        private ObjectType type;
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

        public Builder setType(ObjectType type) {
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
