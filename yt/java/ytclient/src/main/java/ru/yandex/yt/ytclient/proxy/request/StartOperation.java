package ru.yandex.yt.ytclient.proxy.request;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpcproxy.EOperationType;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class StartOperation extends RequestBase<StartOperation> {
    private final EOperationType type;
    private final YTreeNode spec;

    private TransactionalOptions transactionalOptions;
    private MutatingOptions mutatingOptions;

    public StartOperation(EOperationType type, YTreeNode spec) {
        this.type = type;
        this.spec = spec;
    }

    public StartOperation setTransactionOptions(TransactionalOptions transactionalOptions) {
        this.transactionalOptions = transactionalOptions;
        return this;
    }

    public StartOperation setMutatingOptions(MutatingOptions mutatingOptions) {
        this.mutatingOptions = mutatingOptions;
        return this;
    }

    public TReqStartOperation.Builder writeTo(TReqStartOperation.Builder builder) {
        ByteString.Output output = ByteString.newOutput();

        YTreeBinarySerializer.serialize(spec, output);

        builder
                .setType(type)
                .setSpec(output.toByteString());

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }

        if (mutatingOptions != null) {
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }

        return builder;
    }
}
