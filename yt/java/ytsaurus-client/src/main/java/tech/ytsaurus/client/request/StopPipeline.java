package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqStopPipeline;

/**
 * Immutable Flow stop pipeline request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#stopPipeline(StopPipeline)
 */
public class StopPipeline
        extends RequestBase<StopPipeline.Builder, StopPipeline>
        implements HighLevelRequest<TReqStopPipeline.Builder> {

    private final YPath pipelinePath;

    public StopPipeline(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
    }

    /**
     * Construct stop pipeline request from path.
     * <p>
     *
     * @param pipelinePath A path to the Flow pipeline.
     */
    public StopPipeline(YPath pipelinePath) {
        this(builder().setPipelinePath(pipelinePath));
    }

    /**
     * Construct empty builder for stop pipeline request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStopPipeline.Builder, ?> builder) {
        builder.body().setPipelinePath(ByteString.copyFromUtf8(pipelinePath.toString()));
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("pipelinePath: ").append(pipelinePath).append(";");
        super.writeArgumentsLogString(sb);
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

    public static class Builder extends RequestBase.Builder<Builder, StopPipeline> {
        @Nullable
        private YPath pipelinePath;

        Builder() {
        }

        /**
         * Sets the path to the Flow pipeline to be stopped.
         * <p>
         *
         * @param pipelinePath A path to the Flow pipeline.
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
         * Construct {@link StopPipeline} instance.
         */
        @Override
        public StopPipeline build() {
            return new StopPipeline(this);
        }
    }
}
