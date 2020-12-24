package ru.yandex.yt.ytclient.proxy.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TReqGetOperation;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public class GetOperation extends RequestBase<GetOperation> implements HighLevelRequest<TReqGetOperation.Builder> {
    private final GUID guid;
    private final List<String> attributes = new ArrayList<>();

    @Nullable
    private MasterReadOptions masterReadOptions;
    private boolean includeRuntime;

    public GetOperation(GUID guid) {
        this.guid = guid;
    }

    public GetOperation setMasterReadOptions(MasterReadOptions masterReadOptions) {
        this.masterReadOptions = masterReadOptions;
        return this;
    }

    public GetOperation includeRuntime(boolean includeRuntime) {
        this.includeRuntime = includeRuntime;
        return this;
    }

    public GetOperation addAttribute(String attribute) {
        this.attributes.add(attribute);
        return this;
    }

    public GetOperation setAttributes(Collection<String> attributes) {
        this.attributes.clear();
        this.attributes.addAll(attributes);
        return this;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetOperation.Builder, ?> requestBuilder) {
        TReqGetOperation.Builder builder = requestBuilder.body()
                .setOperationId(RpcUtil.toProto(guid))
                .addAllAttributes(attributes)
                .setIncludeRuntime(includeRuntime);

        if (masterReadOptions != null) {
            builder.setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
    }

    @Nonnull
    @Override
    protected GetOperation self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Id: ").append(guid).append("; ");
        if (!attributes.isEmpty()) {
            sb.append("Attributes: ").append(attributes).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }
}
