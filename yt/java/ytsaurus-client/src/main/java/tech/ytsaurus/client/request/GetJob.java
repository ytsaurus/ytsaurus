package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TReqGetJob;
import tech.ytsaurus.ytree.TAttributeFilter;

public class GetJob extends OperationReq<GetJob.Builder, GetJob> implements HighLevelRequest<TReqGetJob.Builder> {
    private final GUID jobId;
    private final List<String> attributes;

    public GetJob(BuilderBase<?> builder) {
        super(builder);
        this.jobId = Objects.requireNonNull(builder.jobId);
        this.attributes = builder.attributes;
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
        if (!attributes.isEmpty()) {
            messageBuilder.setAttributes(TAttributeFilter.newBuilder().addAllKeys(attributes));
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        if (!attributes.isEmpty()) {
            sb.append("Attributes: ").append(attributes).append("; ");
        }
        super.writeArgumentsLogString(sb);
        sb.append("jobId: ").append(jobId).append(";");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setOperationId(operationId)
                .setOperationAlias(operationAlias)
                .setJobId(jobId)
                .setAttributes(new ArrayList<>(attributes))
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
        private List<String> attributes = new ArrayList<>();

        public BuilderBase() {
        }

        public BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.jobId = builder.jobId;
            this.attributes = new ArrayList<>(builder.attributes);
        }

        public TBuilder setJobId(GUID jobId) {
            this.jobId = jobId;
            return self();
        }

        public TBuilder addAttribute(String attribute) {
            this.attributes.add(attribute);
            return self();
        }

        public TBuilder setAttributes(Collection<String> attributes) {
            this.attributes.clear();
            this.attributes.addAll(attributes);
            return self();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            if (!attributes.isEmpty()) {
                sb.append("Attributes: ").append(attributes).append("; ");
            }
            super.writeArgumentsLogString(sb);
            sb.append("jobId: ").append(jobId).append(";");
        }

        @Override
        public GetJob build() {
            return new GetJob(this);
        }
    }
}
