package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Request for pinging transaction.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#ping_tx">
 *     ping_tx documentation
 *     </a>
 */
@NonNullApi
@NonNullFields
public class PingTransaction
        extends RequestBase<PingTransaction>
        implements HighLevelRequest<TReqPingTransaction.Builder> {
    final GUID transactionId;
    boolean pingAncestors = TReqPingTransaction.getDefaultInstance().getPingAncestors();

    public PingTransaction(GUID transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * If set to true, not only the specified transaction but also all its ancestors will be pinged.
     * If set to false only specified transaction will be pinged.
     *
     * Default value: true.
     */
    public PingTransaction setPingAncestors(boolean pingAncestors) {
        this.pingAncestors = pingAncestors;
        return this;
    }

    /**
     * @see #setPingAncestors
     */
    public boolean getPingAncestors() {
        return pingAncestors;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPingTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
        if (pingAncestors != TReqPingTransaction.getDefaultInstance().getPingAncestors()) {
            builder.body().setPingAncestors(pingAncestors);
        }
    }

    @Nonnull
    @Override
    protected PingTransaction self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("TransactionId: ").append(transactionId).append(";");
    }
}
