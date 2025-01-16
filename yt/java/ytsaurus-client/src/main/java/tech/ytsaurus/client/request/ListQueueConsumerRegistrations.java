package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqListQueueConsumerRegistrations;

/**
 * Immutable list queue consumer registrations request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#listQueueConsumerRegistrations(ListQueueConsumerRegistrations)
 */
public class ListQueueConsumerRegistrations extends RequestBase<ListQueueConsumerRegistrations.Builder,
        ListQueueConsumerRegistrations>
        implements HighLevelRequest<TReqListQueueConsumerRegistrations.Builder> {
    @Nullable
    private final YPath queuePath;
    @Nullable
    private final YPath consumerPath;

    ListQueueConsumerRegistrations(Builder builder) {
        super(builder);
        this.queuePath = builder.queuePath;
        this.consumerPath = builder.consumerPath;
    }

    /**
     * Construct empty builder for list queue consumer registrations request.
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
                .setQueuePath(queuePath)
                .setConsumerPath(consumerPath)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqListQueueConsumerRegistrations.Builder, ?> requestBuilder) {
        TReqListQueueConsumerRegistrations.Builder builder = requestBuilder.body();
        if (queuePath != null) {
            builder.setQueuePath(ByteString.copyFromUtf8(queuePath.toString()));
        }
        if (consumerPath != null) {
            builder.setConsumerPath(ByteString.copyFromUtf8(consumerPath.toString()));
        }
    }

    /**
     * Builder for {@link ListQueueConsumerRegistrations}
     */
    public static class Builder extends RequestBase.Builder<ListQueueConsumerRegistrations.Builder,
            ListQueueConsumerRegistrations> {
        @Nullable
        private YPath queuePath;
        @Nullable
        private YPath consumerPath;

        private Builder() {
        }

        /**
         * Set queue path.
         *
         * @return self
         */
        public Builder setQueuePath(@Nullable YPath queuePath) {
            this.queuePath = queuePath;
            return self();
        }

        /**
         * Set consumer path.
         *
         * @return self
         */
        public Builder setConsumerPath(@Nullable YPath consumerPath) {
            this.consumerPath = consumerPath;
            return self();
        }

        /**
         * Construct {@link ListQueueConsumerRegistrations} instance.
         */
        public ListQueueConsumerRegistrations build() {
            return new ListQueueConsumerRegistrations(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
