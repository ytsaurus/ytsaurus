package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TAttributeKeys;
import ru.yandex.yt.rpcproxy.TMasterReadOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.rpcproxy.TSuppressableAccessTrackingOptions;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class GetNode extends GetLikeReq<GetNode> {
    public GetNode(String path) {
        super(path);
    }

    public TReqGetNode.Builder writeTo(TReqGetNode.Builder builder) {
        builder.setPath(path);
        if (attributes != null) {
            builder.setAttributes(attributes.writeTo(TAttributeKeys.newBuilder()));
        }
        if (maxSize != null) {
            builder.setMaxSize(maxSize);
        }
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
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }
}
