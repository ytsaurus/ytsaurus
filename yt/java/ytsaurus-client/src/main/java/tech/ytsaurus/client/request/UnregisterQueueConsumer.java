package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqUnregisterQueueConsumer;

/**
 * Immutable unregister queue consumer request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#unregisterQueueConsumer(UnregisterQueueConsumer)
 */
public class UnregisterQueueConsumer extends RequestBase<UnregisterQueueConsumer.Builder, UnregisterQueueConsumer>
        implements HighLevelRequest<TReqUnregisterQueueConsumer.Builder> {
    private final YPath consumerPath;
    private final YPath queuePath;

    UnregisterQueueConsumer(Builder builder) {
        super(builder);
        this.consumerPath = Objects.requireNonNull(builder.consumerPath);
        this.queuePath = Objects.requireNonNull(builder.queuePath);
    }

    /**
     * Construct empty builder for unregister queue consumer request.
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
                .setConsumerPath(consumerPath)
                .setQueuePath(queuePath)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("ConsumerPath: ").append(consumerPath).append("; ");
        sb.append("QueuePath: ").append(queuePath).append("; ");
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUnregisterQueueConsumer.Builder, ?> requestBuilder) {
        TReqUnregisterQueueConsumer.Builder builder = requestBuilder.body();
        builder.setQueuePath(ByteString.copyFromUtf8(queuePath.toString()));
        builder.setConsumerPath(ByteString.copyFromUtf8(consumerPath.toString()));
    }

    /**
     * Builder for {@link UnregisterQueueConsumer}
     */
    public static class Builder extends RequestBase.Builder<UnregisterQueueConsumer.Builder, UnregisterQueueConsumer> {
        @Nullable
        private YPath consumerPath;
        @Nullable
        private YPath queuePath;

        private Builder() {
        }

        /**
         * Set consumer path.
         * <p>
         * Path is a rich {@link YPath} and may specify a cluster via additional attribute.
         * For example:
         * {@code YPath.simple("//tmp/consumer").plusAdditionalAttribute("cluster", "my-cluster")}.
         * If cluster is not specified, the client's cluster is used.
         *
         * @return self
         */
        public UnregisterQueueConsumer.Builder setConsumerPath(YPath consumerPath) {
            this.consumerPath = consumerPath;
            return self();
        }

        /**
         * Set queue path.
         * <p>
         * Path is a rich {@link YPath} and may specify a cluster via additional attribute.
         * For example:
         * {@code YPath.simple("//tmp/queue").plusAdditionalAttribute("cluster", "my-cluster")}.
         * If cluster is not specified, the client's cluster is used.
         *
         * @return self
         */
        public UnregisterQueueConsumer.Builder setQueuePath(YPath queuePath) {
            this.queuePath = queuePath;
            return self();
        }

        /**
         * Construct {@link UnregisterQueueConsumer} instance.
         */
        public UnregisterQueueConsumer build() {
            return new UnregisterQueueConsumer(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
