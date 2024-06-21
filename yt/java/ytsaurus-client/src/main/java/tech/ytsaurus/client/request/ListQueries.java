package tech.ytsaurus.client.request;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TReqListQueries;
import tech.ytsaurus.ytree.TAttributeFilter;

/**
 * Immutable list queries request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#listQueries(ListQueries)
 */
public class ListQueries extends QueryTrackerReq<ListQueries.Builder, ListQueries>
        implements HighLevelRequest<TReqListQueries.Builder> {
    @Nullable
    private final Instant fromTime;
    @Nullable
    private final Instant toTime;
    @Nullable
    private final Instant cursorTime;
    private final OperationSortDirection cursorDirection;
    @Nullable
    private final String userFilter;
    @Nullable
    private final QueryState stateFilter;
    @Nullable
    private final QueryEngine engineFilter;
    @Nullable
    private final String substrFilter;
    private final long limit;
    @Nullable
    private final List<String> attributes;

    ListQueries(Builder builder) {
        super(builder);
        this.fromTime = builder.fromTime;
        this.toTime = builder.toTime;
        this.cursorTime = builder.cursorTime;
        this.cursorDirection = Objects.requireNonNull(builder.cursorDirection);
        this.userFilter = builder.userFilter;
        this.stateFilter = builder.stateFilter;
        this.engineFilter = builder.engineFilter;
        this.substrFilter = builder.substrFilter;
        this.limit = builder.limit;
        this.attributes = builder.attributes;
    }

    /**
     * Construct empty builder for list queries request.
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
                .setFromTime(fromTime)
                .setToTime(toTime)
                .setCursorTime(cursorTime)
                .setCursorDirection(cursorDirection)
                .setUserFilter(userFilter)
                .setStateFilter(stateFilter)
                .setEngineFilter(engineFilter)
                .setSubstrFilter(substrFilter)
                .setLimit(limit)
                .setAttributes(attributes)
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
    public void writeTo(RpcClientRequestBuilder<TReqListQueries.Builder, ?> requestBuilder) {
        TReqListQueries.Builder builder = requestBuilder.body();
        writeQueryTrackerDescriptionToProto(builder::setQueryTrackerStage);
        if (fromTime != null) {
            builder.setFromTime(RpcUtil.instantToMicros(fromTime));
        }
        if (toTime != null) {
            builder.setFromTime(RpcUtil.instantToMicros(toTime));
        }
        if (cursorTime != null) {
            builder.setCursorTime(RpcUtil.instantToMicros(cursorTime));
        }
        builder.setCursorDirection(cursorDirection.getProtoValue());
        if (userFilter != null) {
            builder.setUserFilter(userFilter);
        }
        if (stateFilter != null) {
            builder.setStateFilter(stateFilter.getProtoValue());
        }
        if (engineFilter != null) {
            builder.setEngineFilter(engineFilter.getProtoValue());
        }
        if (substrFilter != null) {
            builder.setSubstrFilter(substrFilter);
        }
        builder.setLimit(limit);
        if (attributes != null) {
            builder.setAttributes(TAttributeFilter.newBuilder().addAllKeys(attributes).build());
        }
    }

    /**
     * Builder for {@link ListQueries}
     */
    public static class Builder extends QueryTrackerReq.Builder<ListQueries.Builder, ListQueries> {
        @Nullable
        private Instant fromTime;
        @Nullable
        private Instant toTime;
        @Nullable
        private Instant cursorTime;
        @Nullable
        private OperationSortDirection cursorDirection = OperationSortDirection.Past;
        @Nullable
        private String userFilter;
        @Nullable
        private QueryState stateFilter;
        @Nullable
        private QueryEngine engineFilter;
        @Nullable
        private String substrFilter;
        private long limit = 100;
        @Nullable
        private List<String> attributes;

        private Builder() {
        }

        /**
         * Set lower bound on query start time.
         *
         * @return self
         */
        public Builder setFromTime(@Nullable Instant fromTime) {
            this.fromTime = fromTime;
            return self();
        }

        /**
         * Set upper bound on query start time.
         *
         * @return self
         */
        public Builder setToTime(@Nullable Instant toTime) {
            this.toTime = toTime;
            return self();
        }

        /**
         * Set the cursor stop time, only valid when {@link #cursorDirection} is specified.
         *
         * @return self
         * @see #setCursorDirection(OperationSortDirection)
         */
        public Builder setCursorTime(@Nullable Instant cursorTime) {
            this.cursorTime = cursorTime;
            return self();
        }

        /**
         * Set the sort order for queries by start time.
         * Default value is {@link OperationSortDirection#Past}
         *
         * @return self
         */
        public Builder setCursorDirection(OperationSortDirection cursorDirection) {
            this.cursorDirection = cursorDirection;
            return self();
        }

        /**
         * Set filter by query author.
         *
         * @return self
         */
        public Builder setUserFilter(@Nullable String userFilter) {
            this.userFilter = userFilter;
            return self();
        }

        /**
         * Set filter by query state.
         *
         * @return self
         * @see QueryState
         */
        public Builder setStateFilter(@Nullable QueryState stateFilter) {
            this.stateFilter = stateFilter;
            return self();
        }

        /**
         * Set filter by query engine.
         *
         * @return self
         * @see QueryEngine
         */
        public Builder setEngineFilter(@Nullable QueryEngine engineFilter) {
            this.engineFilter = engineFilter;
            return self();
        }

        /**
         * Set filter by query id, annotations and access control object.
         *
         * @return self
         */
        public Builder setSubstrFilter(@Nullable String substrFilter) {
            this.substrFilter = substrFilter;
            return self();
        }

        /**
         * Set a limit on the number of queries to be returned.
         * Default value is 100.
         *
         * @return self
         */
        public Builder setLimit(long limit) {
            this.limit = limit;
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
         * Construct {@link ListQueries} instance.
         */
        public ListQueries build() {
            return new ListQueries(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
