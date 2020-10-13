package ru.yandex.yt.ytclient.proxy.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.ExceptionUtils;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TPrerequisiteOptions;
import ru.yandex.yt.rpcproxy.TReqSetNode;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class SetNode extends MutateNode<SetNode> implements HighLevelRequest<TReqSetNode.Builder> {
    private final String path;
    private final byte[] value;
    private boolean force;

    public SetNode(String path, byte[]value) {
        this.path = path;
        this.value = value;
        this.force = false;
    }

    public SetNode(YPath path, YTreeNode value) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(value, baos);

            this.path = path.toString();
            this.value = baos.toByteArray();

            baos.close();
        } catch (IOException ex) {
            throw ExceptionUtils.translate(ex);
        }
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqSetNode.Builder, ?> builder) {
        builder.body().setPath(path)
                .setForce(force)
                .setValue(ByteString.copyFrom(value));
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
}
