package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqGetJob;

public class GetJob extends OperationReq<GetJob.Builder, GetJob> implements HighLevelRequest<TReqGetJob.Builder> {
    private final GUID jobId;

    public GetJob(BuilderBase<?> builder) {
        super(builder);
        this.jobId = Objects.requireNonNull(builder.jobId);
    }

    public GetJob(GUID operationId, GUID jobId) {
        this(builder().setOperationId(operationId).setJobId(jobId));
    }

    GetJob(String alias, GUID jobId) {
        this(builder().setOperationAlias(alias).setJobId(jobId));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static GetJob fromAlias(String alias, GUID jobId) {
        return new GetJob(alias, jobId);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetJob.Builder, ?> requestBuilder) {
        TReqGetJob.Builder messageBuilder = requestBuilder.body();
        writeOperationDescriptionToProto(messageBuilder::setOperationId, messageBuilder::setOperationAlias);
        messageBuilder.setJobId(RpcUtil.toProto(jobId));
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("jobId: ").append(jobId).append(";");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias)
                .setJobId(jobId)
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
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends OperationReq.Builder<TBuilder, GetJob> {
        @Nullable
        private GUID jobId;

        public BuilderBase() {
        }

        public BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.jobId = builder.jobId;
        }

        public TBuilder setJobId(GUID jobId) {
            this.jobId = jobId;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("jobId: ").append(jobId).append(";");
        }

        @Override
        public GetJob build() {
            return new GetJob(this);
        }
    }
}
