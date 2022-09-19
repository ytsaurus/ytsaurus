package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TReqAbortTransaction;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
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
        extends RequestBase
        implements HighLevelRequest<TReqAbortTransaction.Builder> {
    final GUID transactionId;

    AbortTransaction(Builder builder) {
        super(builder);
        this.transactionId = Objects.requireNonNull(builder.transactionId);
    }

    public AbortTransaction(GUID transactionId) {
        this(builder().setTransactionId(transactionId));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("TransactionId: ").append(transactionId).append(";");
    }

    public Builder toBuilder() {
        Builder builder = builder().setTransactionId(transactionId)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        return builder;
    }

    public static class Builder extends RequestBase.Builder<Builder> {
        @Nullable
        GUID transactionId;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.transactionId = builder.transactionId;
        }

        Builder setTransactionId(GUID transactionId) {
            this.transactionId = transactionId;
            return self();
        }

        public AbortTransaction build() {
            return new AbortTransaction(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
