package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqLockNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullFields
@NonNullApi
public class LockNode extends MutatePath<LockNode> implements HighLevelRequest<TReqLockNode.Builder> {
    private final LockMode mode;

    private boolean waitable = false;
    private @Nullable String childKey;
    private @Nullable String attributeKey;

    public LockNode(String path, LockMode mode) {
        super(YPath.simple(path));
        this.mode = mode;
    }

    public LockNode(YPath path, LockMode mode) {
        super(path);
        this.mode = mode;
    }

    public LockNode setWaitable(boolean waitable) {
        this.waitable = waitable;
        return this;
    }

    public LockNode setChildKey(@Nullable String childKey) {
        this.childKey = childKey;
        return this;
    }

    public LockNode setAttributeKey(@Nullable String attributeKey) {
        this.attributeKey = attributeKey;
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqLockNode.Builder, ?> builder) {
        builder.body()
                .setPath(path.toString())
                .setMode(mode.value())
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
        if (mutatingOptions != null) {
            builder.body().setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.body().mergeFrom(additionalData);
        }
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


    @Nonnull
    @Override
    protected LockNode self() {
        return this;
    }
}
