package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.EJobState;
import tech.ytsaurus.rpcproxy.TReqListJobs;

public class ListJobs
        extends OperationReq<ListJobs.Builder, ListJobs>
        implements HighLevelRequest<TReqListJobs.Builder> {
    @Nullable
    private final JobState state;
    @Nullable
    private final Long limit;

    ListJobs(Builder builder) {
        super(builder);
        this.state = builder.state;
        this.limit = builder.limit;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias);
        if (state != null) {
            builder.setState(state);
        }
        if (limit != null) {
            builder.setLimit(limit);
        }
        builder.setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        return builder;
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqListJobs.Builder, ?> requestBuilder) {
        TReqListJobs.Builder messageBuilder = requestBuilder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        if (state != null) {
            messageBuilder.setState(EJobState.forNumber(state.getProtoValue()));
        }
        if (limit != null) {
            messageBuilder.setLimit(limit);
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        if (state != null) {
            sb.append("State: ").append(state.getWireName()).append("; ");
        }
        if (limit != null) {
            sb.append("Limit: ").append(limit).append("; ");
        }
        super.writeArgumentsLogString(sb);
    }

    public static class Builder extends OperationReq.Builder<Builder, ListJobs> {
        @Nullable
        private JobState state;
        @Nullable
        private Long limit;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            operationId = builder.operationId;
            state = builder.state;
            limit = builder.limit;
        }

        public Builder setState(JobState state) {
            this.state = state;
            return self();
        }

        public Builder setLimit(Long limit) {
            this.limit = limit;
            return self();
        }

        public ListJobs build() {
            return new ListJobs(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
