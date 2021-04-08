package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqExistsNode;
import ru.yandex.yt.rpcproxy.TSuppressableAccessTrackingOptions;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class ExistsNode extends GetLikeReq<ExistsNode> implements HighLevelRequest<TReqExistsNode.Builder> {
    public ExistsNode(String path) {
        this(YPath.simple(path));
    }

    public ExistsNode(YPath path) {
        super(path);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqExistsNode.Builder, ?> builder) {
        builder.body().setPath(path.toString());
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
            builder.body().setSuppressableAccessTrackingOptions(suppressableAccessTrackingOptions.writeTo(TSuppressableAccessTrackingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.body().mergeFrom(additionalData);
        }
    }

    @Nonnull
    @Override
    protected ExistsNode self() {
        return this;
    }
}
