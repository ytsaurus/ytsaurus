package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public class TransactionalOptions {
    private GUID transactionId;
    private boolean sticky = false;

    public TransactionalOptions(GUID transactionId, boolean sticky) {
        this.transactionId = transactionId;
        this.sticky = sticky;
    }

    public TransactionalOptions(GUID transactionId) {
        this(transactionId, false);
    }

    public TransactionalOptions() {
    }

    public TransactionalOptions(TransactionalOptions transactionalOptions) {
        transactionId = transactionalOptions.transactionId;
        sticky = transactionalOptions.sticky;
    }

    public boolean getSticky() {
        return sticky;
    }

    public Optional<GUID> getTransactionId() {
        return Optional.ofNullable(transactionId);
    }

    public TransactionalOptions setTransactionId(GUID transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public TTransactionalOptions.Builder writeTo(TTransactionalOptions.Builder builder) {
        if (transactionId != null) {
            builder.setTransactionId(RpcUtil.toProto(transactionId));
        }
        return builder;
    }

    public TTransactionalOptions toProto() {
        return writeTo(TTransactionalOptions.newBuilder()).build();
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (transactionId != null) {
            builder.key("transaction_id").value(transactionId.toString());
        }
        return builder;
    }

    void writeArgumentsLogString(StringBuilder sb) {
        if (transactionId != null) {
            sb.append("TransactionId: ").append(transactionId).append("; ");
        } else {
            sb.append("TransactionId: <null>; ");
        }
    }
}
