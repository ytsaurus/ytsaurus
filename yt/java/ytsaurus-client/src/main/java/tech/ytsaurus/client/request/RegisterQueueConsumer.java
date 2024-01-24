package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqRegisterQueueConsumer;

/**
 * Immutable register queue consumer request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#registerQueueConsumer(RegisterQueueConsumer)
 */
public class RegisterQueueConsumer extends RequestBase<RegisterQueueConsumer.Builder, RegisterQueueConsumer>
        implements HighLevelRequest<TReqRegisterQueueConsumer.Builder> {
    private final YPath consumerPath;
    private final YPath queuePath;
    private final boolean vital;
    @Nullable
    private final RegistrationPartitions registrationPartitions;

    RegisterQueueConsumer(Builder builder) {
        super(builder);
        this.consumerPath = Objects.requireNonNull(builder.consumerPath);
        this.queuePath = Objects.requireNonNull(builder.queuePath);
        this.vital = builder.vital;
        this.registrationPartitions = builder.registrationPartitions;
    }

    /**
     * Construct empty builder for register queue consumer request.
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
                .setVital(vital)
                .setRegistrationPartitions(registrationPartitions);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqRegisterQueueConsumer.Builder, ?> requestBuilder) {
        TReqRegisterQueueConsumer.Builder builder = requestBuilder.body();
        builder.setQueuePath(queuePath.toString());
        builder.setConsumerPath(consumerPath.toString());
        builder.setVital(vital);
        if (registrationPartitions != null) {
            builder.setPartitions(registrationPartitions.toProto());
        }
    }

    /**
     * Builder for {@link RegisterQueueConsumer}
     */
    public static class Builder extends RequestBase.Builder<RegisterQueueConsumer.Builder, RegisterQueueConsumer> {
        @Nullable
        private YPath consumerPath;
        @Nullable
        private YPath queuePath;
        private boolean vital;
        @Nullable
        private RegistrationPartitions registrationPartitions;

        private Builder() {
        }

        public RegisterQueueConsumer.Builder setConsumerPath(YPath consumerPath) {
            this.consumerPath = consumerPath;
            return self();
        }

        public RegisterQueueConsumer.Builder setQueuePath(YPath queuePath) {
            this.queuePath = queuePath;
            return self();
        }

        public RegisterQueueConsumer.Builder setVital(boolean vital) {
            this.vital = vital;
            return self();
        }

        public RegisterQueueConsumer.Builder setRegistrationPartitions(
                @Nullable RegistrationPartitions registrationPartitions
        ) {
            this.registrationPartitions = registrationPartitions;
            return self();
        }

        /**
         * Construct {@link RegisterQueueConsumer} instance.
         */
        public RegisterQueueConsumer build() {
            return new RegisterQueueConsumer(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
