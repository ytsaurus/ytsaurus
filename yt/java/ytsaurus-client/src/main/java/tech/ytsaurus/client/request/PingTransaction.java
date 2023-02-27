package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqPingTransaction;

/**
 * Request for pinging transaction.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#ping_tx">
 * ping_tx documentation
 * </a>
 */
public class PingTransaction extends RequestBase<PingTransaction.Builder, PingTransaction>
        implements HighLevelRequest<TReqPingTransaction.Builder> {
    private final GUID transactionId;
    private final boolean pingAncestors;

    public PingTransaction(GUID transactionId) {
        this(builder().setTransactionId(transactionId));
    }

    PingTransaction(Builder builder) {
        super(builder);
        this.transactionId = Objects.requireNonNull(builder.transactionId);
        this.pingAncestors = builder.pingAncestors;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @see Builder#setPingAncestors
     */
    public boolean getPingAncestors() {
        return pingAncestors;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPingTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));
        if (pingAncestors != TReqPingTransaction.getDefaultInstance().getPingAncestors()) {
            builder.body().setPingAncestors(pingAncestors);
        }
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
                .setPingAncestors(pingAncestors)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, PingTransaction> {
        @Nullable
        private GUID transactionId;
        private boolean pingAncestors = TReqPingTransaction.getDefaultInstance().getPingAncestors();

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            this.transactionId = builder.transactionId;
            this.pingAncestors = builder.pingAncestors;
        }

        public Builder setTransactionId(GUID transactionId) {
            this.transactionId = transactionId;
            return self();
        }

        /**
         * If set to true, not only the specified transaction but also all its ancestors will be pinged.
         * If set to false only specified transaction will be pinged.
         * <b>
         * Default value: true.
         * </b>
         */
        public Builder setPingAncestors(boolean pingAncestors) {
            this.pingAncestors = pingAncestors;
            return self();
        }

        public PingTransaction build() {
            return new PingTransaction(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
