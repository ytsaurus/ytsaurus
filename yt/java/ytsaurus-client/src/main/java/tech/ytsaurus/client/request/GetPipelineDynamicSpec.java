package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqGetPipelineDynamicSpec;

/**
 * Immutable Flow get pipeline dynamic spec request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getPipelineDynamicSpec(GetPipelineDynamicSpec)
 */
public class GetPipelineDynamicSpec
        extends RequestBase<GetPipelineDynamicSpec.Builder, GetPipelineDynamicSpec>
        implements HighLevelRequest<TReqGetPipelineDynamicSpec.Builder> {

    private final YPath pipelinePath;

    public GetPipelineDynamicSpec(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
    }

    /**
     * Construct get pipeline dynamic spec request from path.
     * <p>
     *
     * @param pipelinePath A path to the Flow pipeline.
     */
    public GetPipelineDynamicSpec(YPath pipelinePath) {
        this(builder().setPipelinePath(pipelinePath));
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetPipelineDynamicSpec.Builder, ?> builder) {
        builder.body().setPipelinePath(ByteString.copyFromUtf8(pipelinePath.toString()));
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("pipelinePath: ").append(pipelinePath).append(";");
        super.writeArgumentsLogString(sb);
    }

    /**
     * Construct empty builder for get pipeline dynamic spec request.
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

    public static class Builder extends RequestBase.Builder<Builder, GetPipelineDynamicSpec> {
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
         * Construct {@link GetPipelineDynamicSpec} instance.
         */
        @Override
        public GetPipelineDynamicSpec build() {
            return new GetPipelineDynamicSpec(this);
        }
    }
}
