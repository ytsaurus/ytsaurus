package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TAttributeKeys;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.rpcproxy.TSuppressableAccessTrackingOptions;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class GetNode extends GetLikeReq<GetNode> implements HighLevelRequest<TReqGetNode.Builder> {
    public GetNode(String path) {
        this(YPath.simple(path));
    }

    public GetNode(YPath path) {
        super(path);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetNode.Builder, ?> builder) {
        builder.body().setPath(path.toString());
        if (attributes != null) {
            builder.body().setAttributes(attributes.writeTo(TAttributeKeys.newBuilder()));
        }
        if (maxSize != null) {
            builder.body().setMaxSize(maxSize);
        }
        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.body().setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (masterReadOptions != null) {
            builder.body().setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
        if (suppressableAccessTrackingOptions != null) {
            builder.body().setSuppressableAccessTrackingOptions(
                    suppressableAccessTrackingOptions.writeTo(TSuppressableAccessTrackingOptions.newBuilder())
            );
        }
        if (additionalData != null) {
            builder.body().mergeFrom(additionalData);
        }
    }

    @Nonnull
    @Override
    protected GetNode self() {
        return this;
    }
}
