package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqMultiLookup;

public class MultiLookupRowsRequest 
        extends RequestBase<MultiLookupRowsRequest.Builder, MultiLookupRowsRequest> {

    @Nullable
    protected final YtTimestamp timestamp;
    @Nullable
    protected final YtTimestamp retentionTimestamp;
    @Nullable
    protected final ReplicaConsistency replicaConsistency;

    protected final List<MultiLookupRowsSubrequest> subrequests;

    protected MultiLookupRowsRequest(BuilderBase<?> builder) {
        super(builder);
        this.timestamp = builder.timestamp;
        this.retentionTimestamp = builder.retentionTimestamp;
        this.replicaConsistency = builder.replicaConsistency;
        this.subrequests = builder.subrequests;
    }

    public MultiLookupRowsRequest() {
        this(builder());
    }

    /**
     * Get timestamp parameter.
     *
     * @see MultiLookupRowsRequest.Builder#setTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    /**
     * Get retention-timestamp parameter.
     *
     * @see MultiLookupRowsRequest.Builder#setRetentionTimestamp(YtTimestamp)
     */
    public Optional<YtTimestamp> getRetentionTimestamp() {
        return Optional.ofNullable(retentionTimestamp);
    }

    /**
     * Get replica-consistency parameter.
     *
     * @see MultiLookupRowsRequest.Builder#setReplicaConsistency(ReplicaConsistency)
     */
    public Optional<ReplicaConsistency> getReplicaConsistency() {
        return Optional.ofNullable(replicaConsistency);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get list of subrequests.
     *
     * @see Builder#addSubrequest(MultiLookupRowsSubrequest)
     */
    public List<MultiLookupRowsSubrequest> getSubrequests() {
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
                return MultiLookupRowsRequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                MultiLookupRowsRequest.this.writeHeaderTo(header);
            }

            /**
             * Internal method: prepare request to send over network.
             */
            @Override
            public void writeTo(RpcClientRequestBuilder<TReqMultiLookup.Builder, ?> builder) {

                if (getTimestamp().isPresent()) {
                    builder.body().setTimestamp(getTimestamp().get().getValue());
                }
                if (getRetentionTimestamp().isPresent()) {
                    builder.body().setRetentionTimestamp(getRetentionTimestamp().get().getValue());
                }
                if (getReplicaConsistency().isPresent()) {
                    builder.body().setReplicaConsistency(getReplicaConsistency().get().getProtoValue());
                }

                for (var subrequest : subrequests) {
                    subrequest.asMultiLookupRowsSubrequestWritable().writeTo(builder);
                }

            }
        };
    }
    
    @Override
    public Builder toBuilder() {
        return builder()
                .setTimestamp(timestamp)
                .setRetentionTimestamp(retentionTimestamp)
                .setReplicaConsistency(replicaConsistency)
                .setSubrequests(subrequests)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public MultiLookupRowsRequest build() {
            return new MultiLookupRowsRequest(this);
        }
    }
    
    /**
     * Base class for builders of LookupRows requests.
     */
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, MultiLookupRowsRequest> {

        @Nullable
        private YtTimestamp timestamp;
        @Nullable
        private YtTimestamp retentionTimestamp;
        @Nullable
        private ReplicaConsistency replicaConsistency;

        private List<MultiLookupRowsSubrequest> subrequests = new ArrayList<>();

        /**
         * Construct empty builder.
         */
        public BuilderBase() {
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
         * Add subrequest of multilookup request.
         */
        public TBuilder addSubrequest(MultiLookupRowsSubrequest MultiLookupRowsSubrequest) {
            this.subrequests.add(MultiLookupRowsSubrequest);
            return self();
        }

        /**
         * Set subrequests of multilookup request.
         */
        public TBuilder setSubrequests(List<MultiLookupRowsSubrequest> MultiLookupRowsSubrequests) {
            this.subrequests = MultiLookupRowsSubrequests;
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
         * Get subrequests of multilookup request.
         *
         * @see #setSubrequests
         */
        public List<MultiLookupRowsSubrequest> getSubrequests() {
            return subrequests;
        }

    }
}
