package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;

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
public class CommitTransaction extends RequestBase<CommitTransaction.Builder, CommitTransaction>
        implements HighLevelRequest<TReqCommitTransaction.Builder> {
    private final GUID transactionId;

    CommitTransaction(Builder builder) {
        super(builder);
        transactionId = Objects.requireNonNull(builder.transactionId);
    }

    public CommitTransaction(GUID transactionId) {
        this(builder().setTransactionId(transactionId));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCommitTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("TransactionId: ").append(transactionId).append(";");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setTransactionId(transactionId)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends RequestBase.Builder<Builder, CommitTransaction> {
        @Nullable
        private GUID transactionId;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            transactionId = builder.transactionId;
        }

        public Builder setTransactionId(GUID transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public CommitTransaction build() {
            return new CommitTransaction(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
