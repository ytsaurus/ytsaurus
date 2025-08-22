package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqGetPipelineState;

/**
 * Immutable Flow get pipeline state request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getPipelineState(GetPipelineState)
 */
public class GetPipelineState
        extends RequestBase<GetPipelineState.Builder, GetPipelineState>
        implements HighLevelRequest<TReqGetPipelineState.Builder> {
    private final YPath pipelinePath;

    public GetPipelineState(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
    }

    /**
     * Construct get pipeline state request from path.
     * <p>
     *
     * @param pipelinePath A path to the Flow pipeline.
     */
    public GetPipelineState(YPath pipelinePath) {
        this(builder().setPipelinePath(pipelinePath));
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetPipelineState.Builder, ?> builder) {
        builder.body().setPipelinePath(ByteString.copyFromUtf8(pipelinePath.toString()));
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("pipelinePath: ").append(pipelinePath).append(";");
        super.writeArgumentsLogString(sb);
    }

    /**
     * Construct empty builder for get pipeline state request.
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

    public static class Builder extends RequestBase.Builder<Builder, GetPipelineState> {
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
        protected Builder self() {
            return this;
        }

        /**
         * Construct {@link GetPipelineState} instance.
         */
        @Override
        public GetPipelineState build() {
            return new GetPipelineState(this);
        }
    }
}
