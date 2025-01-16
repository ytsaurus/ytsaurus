package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqLookupRows;
import tech.ytsaurus.rpcproxy.TReqVersionedLookupRows;

/**
 * Base class for lookup rows requests.
 * <p>
 * Users use one of the inheritors of this class.
 * <p>
 *
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#lookup_rows">
 * lookup_rows documentation
 * </a>
 */
public abstract class AbstractLookupRowsRequest<
        TBuilder extends AbstractLookupRowsRequest.Builder<TBuilder, TRequest>,
        TRequest extends AbstractLookupRowsRequest<TBuilder, TRequest>>
        extends AbstractLookupRequest<TBuilder, TRequest> {

    @Nullable
    protected final YtTimestamp timestamp;
    @Nullable
    protected final YtTimestamp retentionTimestamp;
    @Nullable
    protected final ReplicaConsistency replicaConsistency;

    protected AbstractLookupRowsRequest(Builder<?, ?> builder) {
        super(builder);
        this.timestamp = builder.timestamp;
        this.retentionTimestamp = builder.retentionTimestamp;
        this.replicaConsistency = builder.replicaConsistency;
    }

    /**
     * Get timestamp parameter.
     *
     * @see AbstractLookupRowsRequest.Builder#setTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    /**
     * Get retention-timestamp parameter.
     *
     * @see AbstractLookupRowsRequest.Builder#setRetentionTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getRetentionTimestamp() {
        return Optional.ofNullable(retentionTimestamp);
    }

    /**
     * Get replica-consistency parameter.
     *
     * @see AbstractLookupRowsRequest.Builder#setReplicaConsistency(ReplicaConsistency)
     */
    public Optional<ReplicaConsistency> getReplicaConsistency() {
        return Optional.ofNullable(replicaConsistency);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    public HighLevelRequest<TReqLookupRows.Builder> asLookupRowsWritable() {
        //noinspection Convert2Diamond
        return new HighLevelRequest<TReqLookupRows.Builder>() {
            @Override
            public String getArgumentsLogString() {
                return AbstractLookupRowsRequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                AbstractLookupRowsRequest.this.writeHeaderTo(header);
            }

            /**
             * Internal method: prepare request to send over network.
             */
            @Override
            public void writeTo(RpcClientRequestBuilder<TReqLookupRows.Builder, ?> builder) {
                builder.body().setPath(ByteString.copyFromUtf8(getPath()));
                builder.body().addAllColumns(getLookupColumns());
                builder.body().setKeepMissingRows(getKeepMissingRows());
                if (getTimestamp().isPresent()) {
                    builder.body().setTimestamp(getTimestamp().get().getValue());
                }
                if (getRetentionTimestamp().isPresent()) {
                    builder.body().setRetentionTimestamp(getRetentionTimestamp().get().getValue());
                }
                if (getReplicaConsistency().isPresent()) {
                    builder.body().setReplicaConsistency(getReplicaConsistency().get().getProtoValue());
                }
                builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(getSchema()));
                serializeRowsetTo(builder.attachments());
            }
        };
    }

    /**
     * Internal method: prepare request to send over network.
     */
    public HighLevelRequest<TReqVersionedLookupRows.Builder> asVersionedLookupRowsWritable() {
        //noinspection Convert2Diamond
        return new HighLevelRequest<TReqVersionedLookupRows.Builder>() {
            @Override
            public String getArgumentsLogString() {
                return AbstractLookupRowsRequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                AbstractLookupRowsRequest.this.writeHeaderTo(header);
            }

            /**
             * Internal method: prepare request to send over network.
             */
            @Override
            public void writeTo(RpcClientRequestBuilder<TReqVersionedLookupRows.Builder, ?> builder) {
                builder.body().setPath(ByteString.copyFromUtf8(getPath()));
                builder.body().addAllColumns(getLookupColumns());
                builder.body().setKeepMissingRows(getKeepMissingRows());
                if (getTimestamp().isPresent()) {
                    builder.body().setTimestamp(getTimestamp().get().getValue());
                }
                builder.body().setRowsetDescriptor(ApiServiceUtil.makeRowsetDescriptor(getSchema()));
                serializeRowsetTo(builder.attachments());
            }
        };
    }

    /**
     * Base class for builders of LookupRows requests.
     */
    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends AbstractLookupRowsRequest<?, TRequest>>
            extends AbstractLookupRequest.Builder<TBuilder, TRequest> {

        @Nullable
        private YtTimestamp timestamp;
        @Nullable
        private YtTimestamp retentionTimestamp;
        @Nullable
        private ReplicaConsistency replicaConsistency;

        /**
         * Construct empty builder.
         */
        public Builder() {
        }

        /**
         * Set version of a table to be used for lookup request.
         */
        public TBuilder setTimestamp(@Nullable YtTimestamp timestamp) {
            this.timestamp = timestamp;
            return self();
        }

        /**
         * Set lower boundary for value timestamps to be returned.
         * I.e. values that were written before this timestamp are ignored and not returned.
         */
        public TBuilder setRetentionTimestamp(@Nullable YtTimestamp retentionTimestamp) {
            this.retentionTimestamp = retentionTimestamp;
            return self();
        }

        /**
         * Set requested read consistency for chaos replicas.
         */
        public TBuilder setReplicaConsistency(@Nullable ReplicaConsistency replicaConsistency) {
            this.replicaConsistency = replicaConsistency;
            return self();
        }

        /**
         * Get value of timestamp parameter.
         *
         * @see #setTimestamp parameter
         */
        public Optional<YtTimestamp> getTimestamp() {
            return Optional.ofNullable(timestamp);
        }

        /**
         * Get value of retention-timestamp parameter.
         *
         * @see #setRetentionTimestamp
         */
        public Optional<YtTimestamp> getRetentionTimestamp() {
            return Optional.ofNullable(retentionTimestamp);
        }

        /**
         * Get value of requested read consistency for chaos replicas.
         *
         * @see #setReplicaConsistency
         */
        public Optional<ReplicaConsistency> getReplicaConsistency() {
            return Optional.ofNullable(replicaConsistency);
        }

    }
}
