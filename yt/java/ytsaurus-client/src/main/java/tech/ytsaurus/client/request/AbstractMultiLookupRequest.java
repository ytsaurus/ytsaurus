package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqMultiLookup;

/**
 * Base class for multi lookup requests.
 */
public abstract class AbstractMultiLookupRequest<
        TBuilder extends AbstractMultiLookupRequest.Builder<TBuilder, TRequest>,
        TRequest extends AbstractMultiLookupRequest<TBuilder, TRequest>> extends RequestBase<TBuilder, TRequest> {

    @Nullable
    protected final YtTimestamp timestamp;
    @Nullable
    protected final YtTimestamp retentionTimestamp;
    @Nullable
    protected final ReplicaConsistency replicaConsistency;
    protected final List<LookupRowsRequest> subrequests;

    protected AbstractMultiLookupRequest(Builder<?, ?> builder) {
        super(builder);
        this.timestamp = builder.timestamp;
        this.retentionTimestamp = builder.retentionTimestamp;
        this.replicaConsistency = builder.replicaConsistency;
        this.subrequests = builder.subrequests;
    }

    /**
     * Get timestamp parameter.
     *
     * @see Builder#setTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    /**
     * Get retention-timestamp parameter.
     *
     * @see Builder#setRetentionTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getRetentionTimestamp() {
        return Optional.ofNullable(retentionTimestamp);
    }

    /**
     * Get replica-consistency parameter.
     *
     * @see Builder#setReplicaConsistency(ReplicaConsistency)
     */
    public Optional<ReplicaConsistency> getReplicaConsistency() {
        return Optional.ofNullable(replicaConsistency);
    }

    /**
     * Get list of subrequests.
     *
     * @see Builder#addSubrequest(LookupRowsRequest)
     */
    public List<LookupRowsRequest> getSubrequests() {
        return subrequests;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    public HighLevelRequest<TReqMultiLookup.Builder> asMultiLookupWritable() {
        //noinspection Convert2Diamond
        return new HighLevelRequest<TReqMultiLookup.Builder>() {
            @Override
            public String getArgumentsLogString() {
                return AbstractMultiLookupRequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                AbstractMultiLookupRequest.this.writeHeaderTo(header);
            }

            /**
             * Internal method: prepare request to send over network.
             */
            @Override
            public void writeTo(RpcClientRequestBuilder<TReqMultiLookup.Builder, ?> builder) {

                for (var subrequest : subrequests) {
                    List<byte[]> subAttachments = new ArrayList<>();
                    subrequest.serializeRowsetTo(subAttachments);
                    builder.attachments().addAll(subAttachments);

                    var rowset = ApiServiceUtil.makeRowsetDescriptor(subrequest.getSchema());

                    builder.body().addSubrequests(
                            TReqMultiLookup.TSubrequest.newBuilder()
                            .setPath(subrequest.getPath())
                            .addAllColumns(subrequest.getLookupColumns())
                            .setKeepMissingRows(subrequest.getKeepMissingRows())
                            .setRowsetDescriptor(rowset)
                            .setAttachmentCount(rowset.getSerializedSize())
                            .build()
                    );
                }

                if (getTimestamp().isPresent()) {
                    builder.body().setTimestamp(getTimestamp().get().getValue());
                }
                if (getRetentionTimestamp().isPresent()) {
                    builder.body().setRetentionTimestamp(getRetentionTimestamp().get().getValue());
                }
                if (getReplicaConsistency().isPresent()) {
                    builder.body().setReplicaConsistency(getReplicaConsistency().get().getProtoValue());
                }

            }
        };
    }

    /**
     * Base class for builders of LookupRows requests.
     */
    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends AbstractMultiLookupRequest<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {

        @Nullable
        private YtTimestamp timestamp;
        @Nullable
        private YtTimestamp retentionTimestamp;
        @Nullable
        private ReplicaConsistency replicaConsistency;
        private final List<LookupRowsRequest> subrequests = new ArrayList<>();

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

        /**
         * Add subrequest of multilookup request.
         */
        public TBuilder addSubrequest(LookupRowsRequest lookupRowsRequest) {
            this.subrequests.add(lookupRowsRequest);
            return self();
        }

    }
}
