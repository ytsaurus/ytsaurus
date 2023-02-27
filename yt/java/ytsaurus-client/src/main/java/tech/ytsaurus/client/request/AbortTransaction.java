package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqAbortTransaction;

/**
 * Immutable abort transaction request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#abortTransaction(AbortTransaction)
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#abort_tx">
 * abort_tx documentation
 * </a>
 */
public class AbortTransaction
        extends RequestBase<AbortTransaction.Builder, AbortTransaction>
        implements HighLevelRequest<TReqAbortTransaction.Builder> {
    final GUID transactionId;

    /**
     * Construct abort transaction request from transaction id with other options set to defaults.
     */
    public AbortTransaction(GUID transactionId) {
        this(builder().setTransactionId(transactionId));
    }

    AbortTransaction(Builder builder) {
        super(builder);
        this.transactionId = Objects.requireNonNull(builder.transactionId);
    }

    /**
     * Construct empty builder for abort transaction request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("TransactionId: ").append(transactionId).append(";");
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        return builder().setTransactionId(transactionId)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    /**
     * Builder for {@link AbortTransaction}
     */
    public static class Builder extends RequestBase.Builder<Builder, AbortTransaction> {
        @Nullable
        GUID transactionId;

        Builder() {
        }

        /**
         * Set transaction id.
         */
        public Builder setTransactionId(GUID transactionId) {
            this.transactionId = transactionId;
            return self();
        }

        /**
         * Construct {@link AbortTransaction} instance.
         */
        public AbortTransaction build() {
            return new AbortTransaction(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
