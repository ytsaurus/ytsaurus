package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTreeBuilder;

public class TransactionalOptions {
    @Nullable
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

    public YTreeBuilder toTree(YTreeBuilder builder) {
        if (transactionId != null) {
            builder.key("transaction_id").value(transactionId.toString());
        }
        return builder;
    }

    public void writeArgumentsLogString(StringBuilder sb) {
        if (transactionId != null) {
            sb.append("TransactionId: ").append(transactionId).append("; ");
        } else {
            sb.append("TransactionId: <null>; ");
        }
    }
}
