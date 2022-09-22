package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
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
public class PingTransaction extends RequestBase<PingTransaction.Builder>
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
     * @see #Builder.setPingAncestors
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

    @NonNullApi
    @NonNullFields
    public static class Builder extends RequestBase.Builder<Builder> {
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
         *
         * Default value: true.
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
