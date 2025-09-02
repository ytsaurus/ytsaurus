package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqPullQueueConsumer;

/**
 * Immutable pull consumer request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#pullConsumer(PullConsumer)
 */
public class PullConsumer extends RequestBase<PullConsumer.Builder, PullConsumer>
        implements HighLevelRequest<TReqPullQueueConsumer.Builder> {
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
        return builder()
                .setConsumerPath(consumerPath)
                .setQueuePath(queuePath)
                .setPartitionIndex(partitionIndex)
                .setOffset(offset)
                .setRowBatchReadOptions(rowBatchReadOptions)
                .setReplicaConsistency(replicaConsistency)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("consumerPath: ").append(consumerPath).append(";");
        sb.append("queuePath: ").append(queuePath).append(";");
        sb.append("partitionIndex: ").append(partitionIndex).append(";");
        if (offset != null) {
            sb.append("offset: ").append(offset).append(";");
        }
        if (replicaConsistency != null) {
            sb.append("replicaConsistency: ").append(replicaConsistency).append(";");
        }
        sb.append("rowBatchReadOptions: ").append(rowBatchReadOptions).append(";");
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPullQueueConsumer.Builder, ?> requestBuilder) {
        TReqPullQueueConsumer.Builder builder = requestBuilder.body();
        builder.setConsumerPath(ByteString.copyFromUtf8(consumerPath.toString()));
        builder.setQueuePath(ByteString.copyFromUtf8(queuePath.toString()));
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

        public PullConsumer.Builder setOffset(@Nullable Long offset) {
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

        public PullConsumer.Builder setReplicaConsistency(@Nullable ReplicaConsistency replicaConsistency) {
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
