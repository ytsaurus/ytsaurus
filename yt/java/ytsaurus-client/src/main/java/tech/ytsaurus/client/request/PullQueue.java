package tech.ytsaurus.client.request;


import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqPullQueue;

/**
 * Immutable pull queue request.
 * It almost the same to {@link tech.ytsaurus.client.request.PullConsumer} except that {@link PullQueue} doesn't
 * required consumer.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#pullQueue(PullQueue)
 */
public class PullQueue extends RequestBase<PullQueue.Builder, PullQueue>
        implements HighLevelRequest<TReqPullQueue.Builder> {
    private final YPath queuePath;
    private final int partitionIndex;
    private final RowBatchReadOptions rowBatchReadOptions;
    @Nullable
    private final Long offset;
    @Nullable
    private final ReplicaConsistency replicaConsistency;

    PullQueue(Builder builder) {
        super(builder);
        this.queuePath = Objects.requireNonNull(builder.queuePath);
        this.partitionIndex = Objects.requireNonNull(builder.partitionIndex);
        this.rowBatchReadOptions = Objects.requireNonNull(builder.rowBatchReadOptions);
        this.offset = builder.offset;
        this.replicaConsistency = builder.replicaConsistency;
    }

    /**
     * Construct empty builder for pull queue request.
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
    public void writeTo(RpcClientRequestBuilder<TReqPullQueue.Builder, ?> requestBuilder) {
        TReqPullQueue.Builder builder = requestBuilder.body();
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
     * Builder for {@link PullQueue}
     */
    public static class Builder extends RequestBase.Builder<Builder, PullQueue> {
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

        /**
         * Set path to the QYT.
         * <p>
         *
         * @param queuePath Path.
         * @return self
         */
        public Builder setQueuePath(YPath queuePath) {
            this.queuePath = queuePath;
            return self();
        }

        /**
         * Set offset to start reading from.
         * <p>
         *
         * @param offset Offset value.
         * @return self
         */
        public Builder setOffset(@Nullable Long offset) {
            this.offset = offset;
            return self();
        }

        /**
         * Set partition index to read from.
         * <p>
         *
         * @param partitionIndex Partition index.
         * @return self
         */
        public Builder setPartitionIndex(int partitionIndex) {
            this.partitionIndex = partitionIndex;
            return self();
        }

        /**
         * Set additional row batch read options.
         * Including maxRowCount, maxDataWeight and dataWeightPerRowHint.
         * <p>
         *
         * @param options Row batch read options.
         * @return self
         */
        public Builder setRowBatchReadOptions(RowBatchReadOptions options) {
            this.rowBatchReadOptions = options;
            return self();
        }

        /**
         * Set replica consistency value (sync or none).
         * <p>
         *
         * @param replicaConsistency Replica consistency.
         * @return self
         */
        public Builder setReplicaConsistency(@Nullable ReplicaConsistency replicaConsistency) {
            this.replicaConsistency = replicaConsistency;
            return self();
        }

        /**
         * Construct {@link PullQueue} instance.
         */
        public PullQueue build() {
            return new PullQueue(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
