package ru.yandex.yt.ytclient.proxy.request;

import com.google.protobuf.ByteString;

import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqSetNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class SetNode extends MutateNode<SetNode> {
    private final String path;
    private final byte[] value;

    public SetNode(String path, byte[]value) {
        this.path = path;
        this.value = value;
    }

    public TReqSetNode.Builder writeTo(TReqSetNode.Builder builder) {
        builder.setPath(path)
                .setValue(ByteString.copyFrom(value));
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
        return builder;
    }
}
