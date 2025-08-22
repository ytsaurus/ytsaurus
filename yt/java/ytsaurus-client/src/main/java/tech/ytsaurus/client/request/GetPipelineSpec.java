package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqGetPipelineSpec;

/**
 * Immutable Flow get pipeline spec request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getPipelineSpec(GetPipelineSpec)
 */
public class GetPipelineSpec
        extends RequestBase<GetPipelineSpec.Builder, GetPipelineSpec>
        implements HighLevelRequest<TReqGetPipelineSpec.Builder> {

    private final YPath pipelinePath;

    public GetPipelineSpec(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
    }

    /**
     * Construct get pipeline spec request from path.
     * <p>
     *
     * @param pipelinePath A path to the Flow pipeline.
     */
    public GetPipelineSpec(YPath pipelinePath) {
        this(builder().setPipelinePath(pipelinePath));
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetPipelineSpec.Builder, ?> builder) {
        builder.body().setPipelinePath(ByteString.copyFromUtf8(pipelinePath.toString()));
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("pipelinePath: ").append(pipelinePath).append(";");
        super.writeArgumentsLogString(sb);
    }

    /**
     * Construct empty builder for get pipeline spec request.
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
                .setPipelinePath(pipelinePath)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GetPipelineSpec> {
        @Nullable
        private YPath pipelinePath;

        Builder() {
        }

        /**
         * Sets the path to the Flow pipeline.
         * <p>
         *
         * @param pipelinePath Path for the pipeline.
         * @return self
         */
        public Builder setPipelinePath(YPath pipelinePath) {
            this.pipelinePath = pipelinePath;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("pipelinePath: ").append(pipelinePath).append(";");
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Construct {@link GetPipelineSpec} instance.
         */
        @Override
        public GetPipelineSpec build() {
            return new GetPipelineSpec(this);
        }
    }
}
