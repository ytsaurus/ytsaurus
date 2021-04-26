package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpcproxy.EOperationType;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class StartOperation
        extends RequestBase<StartOperation>
        implements HighLevelRequest<TReqStartOperation.Builder> {
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


    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartOperation.Builder, ?> requestBuilder) {
        TReqStartOperation.Builder builder = requestBuilder.body();
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
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("OperationType: ").append(type).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    public void writeHeaderTo(TRequestHeader.Builder header) {
        super.writeHeaderTo(header);
    }

    @Nonnull
    @Override
    protected StartOperation self() {
        return this;
    }
}
