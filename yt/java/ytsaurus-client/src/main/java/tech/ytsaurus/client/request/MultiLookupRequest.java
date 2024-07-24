package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpcproxy.TReqMultiLookup;

public class MultiLookupRequest 
        extends AbstractTabletReadRequest<MultiLookupRequest.Builder, MultiLookupRequest> {
    
    protected final List<MultiLookupSubrequest> subrequests;

    protected MultiLookupRequest(BuilderBase<?> builder) {
        super(builder);
        this.subrequests = builder.subrequests;
    }

    public MultiLookupRequest() {
        this(builder());
    }

    public MultiLookupRequest(List<MultiLookupSubrequest> subrequests) {
        this(builder().setSubrequests(subrequests));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get list of subrequests.
     *
     * @see Builder#addSubrequest(MultiLookupSubrequest)
     */
    public List<MultiLookupSubrequest> getSubrequests() {
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
                return MultiLookupRequest.this.getArgumentsLogString();
            }

            @Override
            public void writeHeaderTo(TRequestHeader.Builder header) {
                MultiLookupRequest.this.writeHeaderTo(header);
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
    
    @Override
    public Builder toBuilder() {
        return builder()
                .setTimestamp(timestamp)
                .setRetentionTimestamp(retentionTimestamp)
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
        public MultiLookupRequest build() {
            return new MultiLookupRequest(this);
        }
    }
    
    /**
     * Base class for builders of LookupRows requests.
     */
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends AbstractTabletReadRequest.Builder<TBuilder, MultiLookupRequest> {
        
        private List<MultiLookupSubrequest> subrequests = new ArrayList<>();

        /**
         * Construct empty builder.
         */
        public BuilderBase() {
        }

        /**
         * Get subrequests of multilookup request.
         */
        public List<MultiLookupSubrequest> getSubrequests() {
            return subrequests;
        }

        /**
         * Add subrequest of multilookup request.
         */
        public TBuilder addSubrequest(MultiLookupSubrequest MultiLookupSubrequest) {
            this.subrequests.add(MultiLookupSubrequest);
            return self();
        }

        /**
         * Set subrequests of multilookup request.
         */
        public TBuilder setSubrequests(List<MultiLookupSubrequest> MultiLookupSubrequests) {
            this.subrequests = MultiLookupSubrequests;
            return self();
        }

    }
}
