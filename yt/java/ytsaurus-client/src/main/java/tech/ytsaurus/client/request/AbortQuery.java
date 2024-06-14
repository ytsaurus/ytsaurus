package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqAbortQuery;

/**
 * Immutable abort query request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#abortQuery(AbortQuery)
 */
public class AbortQuery extends QueryTrackerReq<AbortQuery.Builder, AbortQuery>
        implements HighLevelRequest<TReqAbortQuery.Builder> {
    private final GUID queryId;
    @Nullable
    private final String abortMessage;

    AbortQuery(Builder builder) {
        super(builder);
        this.queryId = Objects.requireNonNull(builder.queryId);
        this.abortMessage = Objects.requireNonNull(builder.abortMessage);
    }

    /**
     * Construct empty builder for abort query request.
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
                .setAbortMessage(abortMessage)
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
    public void writeTo(RpcClientRequestBuilder<TReqAbortQuery.Builder, ?> requestBuilder) {
        TReqAbortQuery.Builder builder = requestBuilder.body();
        writeQueryTrackerDescriptionToProto(builder::setQueryTrackerStage);
        builder.setQueryId(RpcUtil.toProto(queryId));
        if (abortMessage != null) {
            builder.setAbortMessage(abortMessage);
        }
    }

    /**
     * Builder for {@link AbortQuery}
     */
    public static class Builder extends QueryTrackerReq.Builder<AbortQuery.Builder, AbortQuery> {
        @Nullable
        private GUID queryId;
        @Nullable
        private String abortMessage;

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
         * Set abort message.
         *
         * @return self
         */
        public Builder setAbortMessage(@Nullable String message) {
            this.abortMessage = message;
            return self();
        }

        /**
         * Construct {@link AbortQuery} instance.
         */
        public AbortQuery build() {
            return new AbortQuery(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
