package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqAbortJob;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public class AbortJob
        extends RequestBase<AbortJob.Builder, AbortJob>
        implements HighLevelRequest<TReqAbortJob.Builder> {
    private final GUID jobId;
    @Nullable
    private final Long interruptTimeout;

    public AbortJob(GUID jobId) {
        this(builder().setJobId(jobId));
    }

    public AbortJob(BuilderBase<?> builder) {
        super(builder);
        this.jobId = Objects.requireNonNull(builder.jobId);
        this.interruptTimeout = builder.interruptTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAbortJob.Builder, ?> requestBuilder) {
        TReqAbortJob.Builder messageBuilder = requestBuilder.body();
        messageBuilder.setJobId(RpcUtil.toProto(jobId));
        if (interruptTimeout != null) {
            messageBuilder.setInterruptTimeout(interruptTimeout);
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("jobId: ").append(jobId).append(";");
        sb.append("interruptTimeout: ").append(interruptTimeout).append(";");
    }

    @Override
    public Builder toBuilder() {
        Builder builder = builder().setJobId(jobId);
        if (interruptTimeout != null) {
            builder.setInterruptTimeout(interruptTimeout);
        }
        builder.setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        return builder;
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, AbortJob> {
        @Nullable
        private GUID jobId;
        @Nullable
        private Long interruptTimeout;

        public BuilderBase() {
        }

        public BuilderBase(BuilderBase<?> builder) {
            super(builder);
            jobId = builder.jobId;
            interruptTimeout = builder.interruptTimeout;
        }

        public TBuilder setJobId(GUID jobId) {
            this.jobId = jobId;
            return self();
        }

        public TBuilder setInterruptTimeout(Long interruptTimeout) {
            this.interruptTimeout = interruptTimeout;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("jobId: ").append(jobId).append(";");
            sb.append("interruptTimeout: ").append(interruptTimeout).append(";");
        }

        @Override
        public AbortJob build() {
            return new AbortJob(this);
        }
    }
}
