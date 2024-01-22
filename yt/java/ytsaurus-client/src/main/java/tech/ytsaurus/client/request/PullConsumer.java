package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqPullConsumer;

/**
 * Immutable pull consumer request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#pullConsumer(PullConsumer)
 * </a>
 */
public class PullConsumer extends RequestBase<PullConsumer.Builder, PullConsumer>
        implements HighLevelRequest<TReqPullConsumer.Builder> {
    private final YPath consumerPath;
    private final YPath queuePath;
    private final int partitionIndex;
    private final RowBatchReadOptions rowBatchReadOptions;
    @Nullable
    private final Long offset;
    @Nullable
    private final ReplicaConsistency replicaConsistency;

    PullConsumer(Builder builder) {
        super(builder);
        this.consumerPath = Objects.requireNonNull(builder.consumerPath);
        this.queuePath = Objects.requireNonNull(builder.queuePath);
        this.partitionIndex = Objects.requireNonNull(builder.partitionIndex);
        this.rowBatchReadOptions = Objects.requireNonNull(builder.rowBatchReadOptions);
        this.offset = builder.offset;
        this.replicaConsistency = builder.replicaConsistency;
    }

    /**
     * Construct empty builder for pull consumer request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        Builder builder = builder();
        builder.consumerPath = consumerPath;
        builder.queuePath = queuePath;
        builder.partitionIndex = partitionIndex;
        builder.offset = offset;
        builder.rowBatchReadOptions = rowBatchReadOptions;
        builder.replicaConsistency = replicaConsistency;
        return builder;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPullConsumer.Builder, ?> requestBuilder) {
        TReqPullConsumer.Builder builder = requestBuilder.body();
        builder.setConsumerPath(consumerPath.toString());
        builder.setQueuePath(queuePath.toString());
        builder.setPartitionIndex(partitionIndex);
        builder.setRowBatchReadOptions(rowBatchReadOptions.toProto());
        if (offset != null) {
            builder.setOffset(offset);
        }
        if (replicaConsistency != null) {
            builder.setReplicaConsistency(replicaConsistency.getProtoValue());
        }
    }

    /**
     * Builder for {@link PullConsumer}
     */
    public static class Builder extends RequestBase.Builder<PullConsumer.Builder, PullConsumer> {
        @Nullable
        private YPath consumerPath;
        @Nullable
        private YPath queuePath;
        @Nullable
        private Long offset;
        @Nullable
        private Integer partitionIndex;
        private RowBatchReadOptions rowBatchReadOptions = RowBatchReadOptions.builder().build();
        @Nullable
        private ReplicaConsistency replicaConsistency;

        private Builder() {
        }

        public PullConsumer.Builder setConsumerPath(YPath consumerPath) {
            this.consumerPath = consumerPath;
            return self();
        }

        public PullConsumer.Builder setQueuePath(YPath queuePath) {
            this.queuePath = queuePath;
            return self();
        }

        public PullConsumer.Builder setOffset(long offset) {
            this.offset = offset;
            return self();
        }

        public PullConsumer.Builder setPartitionIndex(int partitionIndex) {
            this.partitionIndex = partitionIndex;
            return self();
        }

        public PullConsumer.Builder setRowBatchReadOptions(RowBatchReadOptions options) {
            this.rowBatchReadOptions = options;
            return self();
        }

        public PullConsumer.Builder setReplicaConsistency(ReplicaConsistency replicaConsistency) {
            this.replicaConsistency = replicaConsistency;
            return self();
        }

        /**
         * Construct {@link PullConsumer} instance.
         */
        public PullConsumer build() {
            return new PullConsumer(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
