package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqReadQueryResult;

/**
 * Immutable read query result request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#readQueryResult(ReadQueryResult)
 */
public class ReadQueryResult extends QueryTrackerReq<ReadQueryResult.Builder, ReadQueryResult>
        implements HighLevelRequest<TReqReadQueryResult.Builder> {
    private final GUID queryId;
    private final long resultIndex;
    @Nullable
    private final List<String> columns;
    @Nullable
    private final Long lowerRowIndex;
    @Nullable
    private final Long upperRowIndex;

    ReadQueryResult(Builder builder) {
        super(builder);
        this.queryId = Objects.requireNonNull(builder.queryId);
        this.resultIndex = Objects.requireNonNull(builder.resultIndex);
        this.columns = builder.columns;
        this.lowerRowIndex = builder.lowerRowIndex;
        this.upperRowIndex = builder.upperRowIndex;
    }

    /**
     * Construct empty builder for read query result request.
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
                .setColumns(columns)
                .setLowerRowIndex(lowerRowIndex)
                .setUpperRowIndex(upperRowIndex)
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
    public void writeTo(RpcClientRequestBuilder<TReqReadQueryResult.Builder, ?> requestBuilder) {
        TReqReadQueryResult.Builder builder = requestBuilder.body();
        writeQueryTrackerDescriptionToProto(builder::setQueryTrackerStage);
        builder.setQueryId(RpcUtil.toProto(queryId));
        builder.setResultIndex(resultIndex);
        if (columns != null) {
            builder.setColumns(
                    TReqReadQueryResult.TColumns.newBuilder()
                            .addAllItems(columns)
                            .build()
            );
        }
        if (lowerRowIndex != null) {
            builder.setLowerRowIndex(lowerRowIndex);
        }
        if (upperRowIndex != null) {
            builder.setUpperRowIndex(upperRowIndex);
        }
    }

    /**
     * Builder for {@link ReadQueryResult}
     */
    public static class Builder extends QueryTrackerReq.Builder<ReadQueryResult.Builder, ReadQueryResult> {
        @Nullable
        private GUID queryId;
        @Nullable
        private Long resultIndex;
        @Nullable
        private List<String> columns;
        @Nullable
        private Long lowerRowIndex;
        @Nullable
        private Long upperRowIndex;

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
         * Set list of columns to read.
         *
         * @return self
         */
        public Builder setColumns(@Nullable List<String> columns) {
            this.columns = columns;
            return self();
        }

        /**
         * Set lower row index of the result to read.
         *
         * @return self
         */
        public Builder setLowerRowIndex(@Nullable Long lowerRowIndex) {
            this.lowerRowIndex = lowerRowIndex;
            return self();
        }

        /**
         * Set upper row index of the result to read.
         *
         * @return self
         */
        public Builder setUpperRowIndex(@Nullable Long upperRowIndex) {
            this.upperRowIndex = upperRowIndex;
            return self();
        }

        /**
         * Construct {@link ReadQueryResult} instance.
         */
        public ReadQueryResult build() {
            return new ReadQueryResult(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
