package ru.yandex.yt.ytclient.proxy.request;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqCreateObject;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytree.TAttributeDictionary;

@NonNullApi
@NonNullFields
public class CreateObject extends RequestBase<CreateObject> implements HighLevelRequest<TReqCreateObject.Builder> {
    private final ObjectType type;
    private final Map<String, YTreeNode> attributes = new HashMap<>();
    private boolean ignoreExisting = TReqCreateObject.getDefaultInstance().getIgnoreExisting();

    public CreateObject(ObjectType type) {
        this.type = type;
    }

    public CreateObject setIgnoreExisting(boolean ignoreExisting) {
        this.ignoreExisting = ignoreExisting;
        return this;
    }

    public CreateObject addAttribute(String name, YTreeNode value) {
        this.attributes.put(name, value);
        return this;
    }

    public CreateObject setAttributes(Map<String, YTreeNode> attributes) {
        this.attributes.clear();
        this.attributes.putAll(attributes);
        return this;
    }


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

    @Nonnull
    @Override
    protected CreateObject self() {
        return this;
    }
}
