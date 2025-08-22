package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqStartPipeline;

/**
 * Immutable Flow start pipeline request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#startPipeline(StartPipeline)
 */
public class StartPipeline
        extends RequestBase<StartPipeline.Builder, StartPipeline>
        implements HighLevelRequest<TReqStartPipeline.Builder> {

    private final YPath pipelinePath;

    public StartPipeline(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
    }

    /**
     * Construct start pipeline request from path.
     * <p>
     *
     * @param pipelinePath A path to the Flow pipeline to be started.
     */
    public StartPipeline(YPath pipelinePath) {
        this(builder().setPipelinePath(pipelinePath));
    }

    /**
     * Construct empty builder for start pipeline request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartPipeline.Builder, ?> builder) {
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

    public static class Builder extends RequestBase.Builder<Builder, StartPipeline> {
        @Nullable
        private YPath pipelinePath;

        /**
         * Sets the path to the Flow pipeline to be started.
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
         * Construct {@link StartPipeline} instance.
         */
        @Override
        public StartPipeline build() {
            return new StartPipeline(this);
        }
    }
}
