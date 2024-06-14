package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqGetQueryResult;

/**
 * Immutable get query result request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getQueryResult(GetQueryResult)
 */
public class GetQueryResult extends QueryTrackerReq<GetQueryResult.Builder, GetQueryResult>
        implements HighLevelRequest<TReqGetQueryResult.Builder> {
    private final GUID queryId;
    private final long resultIndex;

    GetQueryResult(Builder builder) {
        super(builder);
        this.queryId = Objects.requireNonNull(builder.queryId);
        this.resultIndex = Objects.requireNonNull(builder.resultIndex);
    }

    /**
     * Construct empty builder for get query result request.
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
                .setResultIndex(resultIndex)
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
    public void writeTo(RpcClientRequestBuilder<TReqGetQueryResult.Builder, ?> requestBuilder) {
        TReqGetQueryResult.Builder builder = requestBuilder.body();
        writeQueryTrackerDescriptionToProto(builder::setQueryTrackerStage);
        builder.setQueryId(RpcUtil.toProto(queryId));
        builder.setResultIndex(resultIndex);
    }

    /**
     * Builder for {@link GetQueryResult}
     */
    public static class Builder extends QueryTrackerReq.Builder<GetQueryResult.Builder, GetQueryResult> {
        @Nullable
        private GUID queryId;
        @Nullable
        private Long resultIndex;

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
         * Set result index.
         *
         * @return self
         */
        public Builder setResultIndex(long resultIndex) {
            this.resultIndex = resultIndex;
            return self();
        }

        /**
         * Construct {@link GetQueryResult} instance.
         */
        public GetQueryResult build() {
            return new GetQueryResult(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
