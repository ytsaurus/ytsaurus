package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqExistsNode;
import ru.yandex.yt.rpcproxy.TSuppressableAccessTrackingOptions;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class ExistsNode extends GetLikeReq<ExistsNode> {
    public ExistsNode(String path) {
        super(path);
    }

    public TReqExistsNode.Builder writeTo(TReqExistsNode.Builder builder) {
        builder.setPath(path);
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (masterReadOptions != null) {
            builder.setMasterReadOptions(masterReadOptions.writeTo(TMasterReadOptions.newBuilder()));
        }
        if (suppressableAccessTrackingOptions != null) {
            builder.setSuppressableAccessTrackingOptions(suppressableAccessTrackingOptions.writeTo(TSuppressableAccessTrackingOptions.newBuilder()));
        }
        builder.mergeFrom(additionalData);

        return builder;
    }
}
