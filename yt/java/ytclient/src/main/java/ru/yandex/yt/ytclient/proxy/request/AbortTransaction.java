package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TReqAbortTransaction;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Request for aborting transaction.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#abort_tx">
 *     abort_tx documentation
 *     </a>
 */
public class AbortTransaction
        extends RequestBase<AbortTransaction>
        implements HighLevelRequest<TReqAbortTransaction.Builder> {
    final GUID transactionId;

    public AbortTransaction(GUID transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
    }

    @Nonnull
    @Override
    protected AbortTransaction self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("TransactionId: ").append(transactionId).append(";");
    }
}
