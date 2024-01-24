package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqAdvanceConsumer;

/**
 * Immutable advance consumer request.
 * <p>
 *
 * @see tech.ytsaurus.client.TransactionalClient#advanceConsumer(AdvanceConsumer)
 */
public class AdvanceConsumer extends RequestBase<AdvanceConsumer.Builder, AdvanceConsumer>
        implements HighLevelRequest<TReqAdvanceConsumer.Builder> {
    @Nullable
    private final GUID transactionId;
    private final YPath consumerPath;
    private final YPath queuePath;
    private final int partitionIndex;
    @Nullable
    private final Long oldOffset;
    private final long newOffset;

    AdvanceConsumer(Builder builder) {
        super(builder);
        this.transactionId = builder.transactionId;
        this.consumerPath = Objects.requireNonNull(builder.consumerPath);
        this.queuePath = Objects.requireNonNull(builder.queuePath);
        this.partitionIndex = Objects.requireNonNull(builder.partitionIndex);
        this.newOffset = Objects.requireNonNull(builder.newOffset);
        this.oldOffset = builder.oldOffset;
    }

    /**
     * Construct empty builder for advance consumer request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        var builder = builder();
        builder.transactionId = transactionId;
        return builder
                .setConsumerPath(consumerPath)
                .setQueuePath(queuePath)
                .setPartitionIndex(partitionIndex)
                .setNewOffset(newOffset)
                .setOldOffset(oldOffset)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAdvanceConsumer.Builder, ?> requestBuilder) {
        TReqAdvanceConsumer.Builder builder = requestBuilder.body();
        if (transactionId == null) {
            throw new RuntimeException("The AdvanceConsumer request requires a transaction ID");
        }

        builder.setTransactionId(RpcUtil.toProto(transactionId));

        builder.setConsumerPath(consumerPath.toString());
        builder.setQueuePath(queuePath.toString());
        builder.setPartitionIndex(partitionIndex);
        if (oldOffset != null) {
            builder.setOldOffset(oldOffset);
        }
        builder.setNewOffset(newOffset);
    }

    /**
     * Builder for {@link AdvanceConsumer}
     */
    public static class Builder extends RequestBase.Builder<AdvanceConsumer.Builder, AdvanceConsumer> {
        @Nullable
        private GUID transactionId;
        @Nullable
        private YPath consumerPath;
        @Nullable
        private YPath queuePath;
        @Nullable
        private Integer partitionIndex;
        @Nullable
        private Long oldOffset;
        @Nullable
        private Long newOffset;

        private Builder() {
        }

        public AdvanceConsumer.Builder setTransactionId(GUID transactionId) {
            this.transactionId = transactionId;
            return self();
        }

        public AdvanceConsumer.Builder setConsumerPath(YPath consumerPath) {
            this.consumerPath = consumerPath;
            return self();
        }

        public AdvanceConsumer.Builder setQueuePath(YPath queuePath) {
            this.queuePath = queuePath;
            return self();
        }

        public AdvanceConsumer.Builder setPartitionIndex(int partitionIndex) {
            this.partitionIndex = partitionIndex;
            return self();
        }

        public AdvanceConsumer.Builder setOldOffset(@Nullable Long oldOffset) {
            this.oldOffset = oldOffset;
            return self();
        }

        public AdvanceConsumer.Builder setNewOffset(long newOffset) {
            this.newOffset = newOffset;
            return self();
        }

        /**
         * Construct {@link AdvanceConsumer} instance.
         */
        public AdvanceConsumer build() {
            return new AdvanceConsumer(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
