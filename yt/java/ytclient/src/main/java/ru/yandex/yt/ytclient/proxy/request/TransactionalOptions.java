package ru.yandex.yt.ytclient.proxy.request;

import java.util.Optional;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public class TransactionalOptions {
    private GUID transactionId;
    private boolean ping = false;
    private boolean pingAncestors = false;
    private boolean sticky = false;

    public TransactionalOptions(GUID transactionId, boolean ping, boolean pingAncestors, boolean sticky) {
        this.transactionId = transactionId;
        this.ping = ping;
        this.pingAncestors = pingAncestors;
        this.sticky = sticky;
    }

    public TransactionalOptions(GUID transactionId) {
        this(transactionId, false, false, false);
    }

    public TransactionalOptions() {

    }

    public TransactionalOptions(TransactionalOptions transactionalOptions) {
        transactionId = transactionalOptions.transactionId;
        ping = transactionalOptions.ping;
        pingAncestors = transactionalOptions.pingAncestors;
        sticky = transactionalOptions.sticky;
    }

    public boolean getSticky() {
        return sticky;
    }

    public TransactionalOptions setSticky(boolean sticky) {
        this.sticky = sticky;
        return this;
    }

    public boolean getPingAncestors() {
        return pingAncestors;
    }

    public TransactionalOptions setPingAncestors(boolean pingAncestors) {
        this.pingAncestors = pingAncestors;
        return this;
    }

    public boolean getPing() {
        return ping;
    }

    public TransactionalOptions setPing(boolean ping) {
        this.ping = ping;
        return this;
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
        builder.setPing(ping);
        builder.setPingAncestors(pingAncestors);
        builder.setSticky(sticky);
        return builder;
    }

    public TTransactionalOptions toProto() {
        return writeTo(TTransactionalOptions.newBuilder()).build();
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (transactionId != null) {
            builder
                    .key("transaction_id").value(transactionId.toString())
                    .key("ping_ancestor_transactions").value(pingAncestors)
            ;
        }
        return builder;
    }
}
