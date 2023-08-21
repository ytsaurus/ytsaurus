package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqGenerateTimestamps;

public class GenerateTimestamps extends RequestBase<GenerateTimestamps.Builder, GenerateTimestamps>
        implements HighLevelRequest<TReqGenerateTimestamps.Builder> {
    private final int count;

    GenerateTimestamps(Builder builder) {
        super(builder);
        this.count = Objects.requireNonNull(builder.count);
    }

    public GenerateTimestamps(int count) {
        this(builder().setCount(count));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGenerateTimestamps.Builder, ?> builder) {
        builder.body().setCount(count);
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("Count: ").append(count).append("; ");
        super.writeArgumentsLogString(sb);
    }

    public Builder toBuilder() {
        return builder()
                .setCount(count)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GenerateTimestamps> {
        @Nullable
        private Integer count;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            count = builder.count;
        }

        public Builder setCount(int count) {
            this.count = count;
            return self();
        }

        public GenerateTimestamps build() {
            return new GenerateTimestamps(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
