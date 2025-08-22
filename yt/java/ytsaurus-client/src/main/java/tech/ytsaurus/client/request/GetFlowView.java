package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqGetFlowView;

/**
 * Immutable Flow get flow view request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getFlowView(GetFlowView)
 */
public class GetFlowView
        extends RequestBase<GetFlowView.Builder, GetFlowView>
        implements HighLevelRequest<TReqGetFlowView.Builder> {
    private final YPath pipelinePath;
    @Nullable
    private final YPath viewPath;
    private final boolean cache;

    public GetFlowView(Builder builder) {
        super(builder);
        this.pipelinePath = Objects.requireNonNull(builder.pipelinePath);
        this.viewPath = builder.viewPath;
        this.cache = builder.cache;
    }

    /**
     * Construct GetFlowView request from pipeline path with other options set to default.
     * <p>
     *
     * @param pipelinePath A path to the Flow pipeline.
     */
    public GetFlowView(YPath pipelinePath) {
        this(builder().setPipelinePath(pipelinePath));
    }

    /**
     * Construct empty builder for get flow view request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetFlowView.Builder, ?> builder) {
        builder.body().setPipelinePath(ByteString.copyFromUtf8(pipelinePath.toString()));
        builder.body().setCache(cache);
        if (viewPath != null) {
            builder.body().setViewPath(ByteString.copyFromUtf8(viewPath.toString()));
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("pipelinePath: ").append(pipelinePath).append(";");
        if (viewPath != null) {
            sb.append("viewPath: ").append(viewPath).append(";");
        }
        sb.append("cache: ").append(cache).append(";");
        super.writeArgumentsLogString(sb);
    }

    /**
     * Construct a builder with options set from this request.
     */
    @Override
    public Builder toBuilder() {
        return builder()
                .setPipelinePath(pipelinePath)
                .setViewPath(viewPath)
                .setCache(cache)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GetFlowView> {
        @Nullable
        private YPath pipelinePath;
        @Nullable
        private YPath viewPath;
        private boolean cache;

        Builder() {
        }

        /**
         * Sets the path to the Flow pipeline to be operated on.
         * <p>
         *
         * @param pipelinePath Path for the pipeline.
         * @return self
         */
        public Builder setPipelinePath(YPath pipelinePath) {
            this.pipelinePath = pipelinePath;
            return self();
        }

        /**
         * Sets the path inside the FlowView (e.g., "/state/execution_spec/layout/partitions").
         * If not set, the full FlowView will be returned, which could be several megabytes in size.
         * <p>
         *
         * @param viewPath Path for the view.
         * @return self
         */
        public Builder setViewPath(@Nullable YPath viewPath) {
            this.viewPath = viewPath;
            return self();
        }

        /**
         * If the flag value is true, the FlowView will be returned from the cache.
         * <p>
         *
         * @param cache Flag.
         * @return self
         */
        public Builder setCache(boolean cache) {
            this.cache = cache;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Construct {@link GetFlowView} instance.
         */
        @Override
        public GetFlowView build() {
            return new GetFlowView(this);
        }
    }
}
