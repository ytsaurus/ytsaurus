package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqPausePipeline;

/**
 * Immutable Flow pause pipeline request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#pausePipeline(PausePipeline)
 */
public class PausePipeline
        extends RequestBase<PausePipeline.Builder, PausePipeline>
        implements HighLevelRequest<TReqPausePipeline.Builder> {

    private final YPath pipelinePath;

    public PausePipeline(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
    }

    /**
     * Construct pause pipeline request from path.
     * <p>
     *
     * @param pipelinePath A path to the Flow pipeline to be paused.
     */
    public PausePipeline(YPath pipelinePath) {
        this(builder().setPipelinePath(pipelinePath));
    }

    /**
     * Construct empty builder for pause pipeline request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPausePipeline.Builder, ?> builder) {
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

    public static class Builder extends RequestBase.Builder<Builder, PausePipeline> {
        @Nullable
        private YPath pipelinePath;

        Builder() {
        }

        /**
         * Sets the path to the Flow pipeline to be paused.
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
         * Construct {@link PausePipeline} instance.
         */
        @Override
        public PausePipeline build() {
            return new PausePipeline(this);
        }
    }
}
