package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqCommitTransaction;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Request for committing transaction.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#commit_tx">
 *     commit_tx documentation
 *     </a>
 */
@NonNullFields
@NonNullApi
public class CommitTransaction
        extends RequestBase<CommitTransaction>
        implements HighLevelRequest<TReqCommitTransaction.Builder> {
    final GUID transactionId;

    public CommitTransaction(GUID transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCommitTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
    }

    @Nonnull
    @Override
    protected CommitTransaction self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("TransactionId: ").append(transactionId).append(";");
    }
}
