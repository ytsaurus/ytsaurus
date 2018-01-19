package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.misc.YtGuid;

public class TransactionalOptions {
    private YtGuid transactionId;
    private boolean ping = false;
    private boolean pingAncestors = false;
    private boolean sticky = false;

    public TransactionalOptions(YtGuid transactionId, boolean ping, boolean pingAncestors, boolean sticky) {
        this.transactionId = transactionId;
        this.ping = ping;
        this.pingAncestors = pingAncestors;
        this.sticky = sticky;
    }

    public TransactionalOptions(YtGuid transactionId) {
        this(transactionId, false, false, false);
    }

    public TransactionalOptions() {

    }

    public TransactionalOptions setSticky(boolean sticky) {
        this.sticky = sticky;
        return this;
    }

    public TransactionalOptions setPingAncestors(boolean pingAncestors) {
        this.pingAncestors = pingAncestors;
        return this;
    }

    public TransactionalOptions setPing(boolean ping) {
        this.ping = ping;
        return this;
    }

    public TransactionalOptions setTransactionId(YtGuid transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public TTransactionalOptions.Builder writeTo(TTransactionalOptions.Builder builder) {
        if (transactionId != null) {
            builder.setTransactionId(transactionId.toProto());
        }
        builder.setPing(ping);
        builder.setPingAncestors(pingAncestors);
        builder.setSticky(sticky);
        return builder;
    }

    public TTransactionalOptions toProto() {
        return writeTo(TTransactionalOptions.newBuilder()).build();
    }
}
