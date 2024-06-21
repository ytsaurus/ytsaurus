package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.rpcproxy.TReqGetQuery;
import tech.ytsaurus.ytree.TAttributeFilter;

/**
 * Immutable get query request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getQuery(GetQuery)
 */
public class GetQuery extends QueryTrackerReq<GetQuery.Builder, GetQuery>
        implements HighLevelRequest<TReqGetQuery.Builder> {
    private final GUID queryId;
    @Nullable
    private final List<String> attributes;
    @Nullable
    private final YtTimestamp timestamp;

    GetQuery(Builder builder) {
        super(builder);
        this.queryId = Objects.requireNonNull(builder.queryId);
        this.attributes = builder.attributes;
        this.timestamp = builder.timestamp;
    }

    /**
     * Construct empty builder for get query request.
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
                .setQueryId(queryId)
                .setAttributes(attributes)
                .setTimestamp(timestamp)
                .setQueryTrackerStage(queryTrackerStage)
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
    public void writeTo(RpcClientRequestBuilder<TReqGetQuery.Builder, ?> requestBuilder) {
        TReqGetQuery.Builder builder = requestBuilder.body();
        writeQueryTrackerDescriptionToProto(builder::setQueryTrackerStage);
        builder.setQueryId(RpcUtil.toProto(queryId));
        if (attributes != null) {
            builder.setAttributes(TAttributeFilter.newBuilder().addAllKeys(attributes).build());
        }
        if (timestamp != null) {
            builder.setTimestamp(timestamp.getValue());
        }
    }

    /**
     * Builder for {@link GetQuery}
     */
    public static class Builder extends QueryTrackerReq.Builder<GetQuery.Builder, GetQuery> {
        @Nullable
        private GUID queryId;
        @Nullable
        private List<String> attributes;
        @Nullable
        private YtTimestamp timestamp;

        private Builder() {
        }

        /**
         * Set query id.
         *
         * @return self
         */
        public Builder setQueryId(GUID queryId) {
            this.queryId = queryId;
            return self();
        }

        /**
         * Set filter for attributes to be returned.
         *
         * @return self
         */
        public Builder setAttributes(@Nullable List<String> attributes) {
            this.attributes = attributes;
            return self();
        }

        /**
         * Set timestamp.
         * The resulting information about the query will be consistent with the YT at this point in time.
         *
         * @return self
         */
        public Builder setTimestamp(@Nullable YtTimestamp timestamp) {
            this.timestamp = timestamp;
            return self();
        }

        /**
         * Construct {@link GetQuery} instance.
         */
        public GetQuery build() {
            return new GetQuery(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
