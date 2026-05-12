package tech.ytsaurus.client.request;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqAttachTransaction;

/**
 * Request for attaching to an existing transaction.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#attach_tx">
 * attach_tx documentation
 * </a>
 */
public class AttachTransaction
        extends RequestBase<AttachTransaction.Builder, AttachTransaction>
        implements HighLevelRequest<TReqAttachTransaction.Builder> {
    private final GUID transactionId;
    private final boolean ping;
    private final boolean pingAncestors;
    @Nullable private final Duration pingPeriod;
    @Nullable private final Duration failedPingRetryPeriod;
    @Nullable private final String pingerAddress;
    @Nullable private final Consumer<Exception> onPingFailed;

    AttachTransaction(Builder builder) {
        super(builder);
        this.transactionId = Objects.requireNonNull(builder.transactionId);
        this.ping = builder.ping;
        this.pingAncestors = builder.pingAncestors;
        this.pingPeriod = builder.pingPeriod;
        this.failedPingRetryPeriod = builder.failedPingRetryPeriod;
        this.pingerAddress = builder.pingerAddress;
        this.onPingFailed = builder.onPingFailed;
    }

    /**
     * Create new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create new builder with specified transaction id.
     */
    public static Builder builder(GUID transactionId) {
        return new Builder().setTransactionId(transactionId);
    }

    /**
     * Get the id of the transaction to attach to.
     */
    public GUID getTransactionId() {
        return transactionId;
    }

    /**
     * Get whether the transaction should be pinged.
     */
    public boolean getPing() {
        return ping;
    }

    /**
     * Get whether ancestor transactions should be pinged.
     */
    public boolean getPingAncestors() {
        return pingAncestors;
    }

    /**
     * Get the ping period.
     *
     * @see Builder#setPingPeriod
     */
    public Optional<Duration> getPingPeriod() {
        return Optional.ofNullable(pingPeriod);
    }

    /**
     * Get the failed ping retry period.
     *
     * @see Builder#setFailedPingRetryPeriod
     */
    public Optional<Duration> getFailedPingRetryPeriod() {
        return Optional.ofNullable(failedPingRetryPeriod);
    }

    /**
     * Get the address of the pinger that will ping this transaction.
     */
    public Optional<String> getPingerAddress() {
        return Optional.ofNullable(pingerAddress);
    }

    /**
     * Get the operation executed on ping failure.
     *
     * @see Builder#setOnPingFailed
     */
    public Optional<Consumer<Exception>> getOnPingFailed() {
        return Optional.ofNullable(onPingFailed);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAttachTransaction.Builder, ?> builder) {
        builder.body().setTransactionId(RpcUtil.toProto(transactionId));

        if (ping != TReqAttachTransaction.getDefaultInstance().getPing()) {
            builder.body().setPing(ping);
        }
        if (pingAncestors != TReqAttachTransaction.getDefaultInstance().getPingAncestors()) {
            builder.body().setPingAncestors(pingAncestors);
        }
        if (pingPeriod != null) {
            builder.body().setPingPeriod(pingPeriod.toMillis());
        }
        if (pingerAddress != null) {
            builder.body().setPingerAddress(pingerAddress);
        }
    }

    @Override
    public Builder toBuilder() {
        return new Builder(this);
    }

    public static class Builder extends RequestBase.Builder<Builder, AttachTransaction> {
        private GUID transactionId;
        private boolean ping = TReqAttachTransaction.getDefaultInstance().getPing();
        private boolean pingAncestors = TReqAttachTransaction.getDefaultInstance().getPingAncestors();
        @Nullable private Duration pingPeriod;
        @Nullable private Duration failedPingRetryPeriod;
        @Nullable private String pingerAddress;
        @Nullable private Consumer<Exception> onPingFailed;

        Builder() { }

        Builder(Builder builder) {
            super(builder);
            this.transactionId = builder.transactionId;
            this.ping = builder.ping;
            this.pingAncestors = builder.pingAncestors;
            this.pingPeriod = builder.pingPeriod;
            this.failedPingRetryPeriod = builder.failedPingRetryPeriod;
            this.pingerAddress = builder.pingerAddress;
            this.onPingFailed = builder.onPingFailed;
        }

        Builder(AttachTransaction attachTransaction) {
            this.timeout = attachTransaction.timeout;
            this.requestId = attachTransaction.requestId;
            this.traceId = attachTransaction.traceId;
            this.traceSampled = attachTransaction.traceSampled;
            this.userAgent = attachTransaction.userAgent;
            this.additionalData = attachTransaction.additionalData;
            this.transactionId = attachTransaction.transactionId;
            this.ping = attachTransaction.ping;
            this.pingAncestors = attachTransaction.pingAncestors;
            this.pingPeriod = attachTransaction.pingPeriod;
            this.failedPingRetryPeriod = attachTransaction.failedPingRetryPeriod;
            this.pingerAddress = attachTransaction.pingerAddress;
            this.onPingFailed = attachTransaction.onPingFailed;
        }

        /**
         * Set the id of the transaction to attach to.
         */
        public Builder setTransactionId(GUID transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        /**
         * Set whether the transaction should be pinged.
         */
        public Builder setPing(boolean ping) {
            this.ping = ping;
            return this;
        }

        /**
         * Set whether ancestor transactions should be pinged.
         */
        public Builder setPingAncestors(boolean pingAncestors) {
            this.pingAncestors = pingAncestors;
            return this;
        }

        /**
         * Set ping period.
         * <p>
         * If ping period is set yt client will automatically ping transaction with specified period.
         */
        public Builder setPingPeriod(@Nullable Duration pingPeriod) {
            this.pingPeriod = pingPeriod;
            return this;
        }

        /**
         * Set failed ping retry period.
         * <p>
         * If transaction ping fails, it will retry with this period.
         *
         * @see #setPingPeriod
         */
        public Builder setFailedPingRetryPeriod(@Nullable Duration failedPingRetryPeriod) {
            this.failedPingRetryPeriod = failedPingRetryPeriod;
            return this;
        }

        /**
         * Set the address of the pinger that will ping this transaction.
         */
        public Builder setPingerAddress(@Nullable String pingerAddress) {
            this.pingerAddress = pingerAddress;
            return this;
        }

        /**
         * Set operation executed on ping failure.
         *
         * @param onPingFailed operation, which will be executed
         */
        public Builder setOnPingFailed(@Nullable Consumer<Exception> onPingFailed) {
            this.onPingFailed = onPingFailed;
            return this;
        }

        @Override
        public AttachTransaction build() {
            return new AttachTransaction(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
