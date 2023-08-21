package tech.ytsaurus.client.request;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.Message;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.tracing.TTracingExt;

public abstract class RequestBase<
        TBuilder extends RequestBase.Builder<TBuilder, TRequest>, TRequest extends RequestBase<TBuilder, TRequest>> {
    protected @Nullable
    Duration timeout;
    protected @Nullable
    GUID requestId;
    protected @Nullable
    GUID traceId;
    protected boolean traceSampled;
    protected String userAgent;
    protected @Nullable
    Message additionalData;

    RequestBase(Builder<?, ?> builder) {
        this.timeout = builder.timeout;
        this.requestId = builder.requestId;
        this.traceId = builder.traceId;
        this.traceSampled = builder.traceSampled;
        this.userAgent = builder.userAgent;
        this.additionalData = builder.additionalData;
    }

    protected RequestBase(RequestBase<?, ?> other) {
        timeout = other.timeout;
        requestId = other.requestId;
        traceId = other.traceId;
        traceSampled = other.traceSampled;
        userAgent = other.userAgent;
        additionalData = other.additionalData;
    }

    @SuppressWarnings("unused")
    @Nullable
    Message getAdditionalData() {
        return additionalData;
    }

    public Optional<Duration> getTimeout() {
        return Optional.ofNullable(timeout);
    }

    @SuppressWarnings("unused")
    public Optional<GUID> getRequestId() {
        return Optional.ofNullable(requestId);
    }

    @SuppressWarnings("unused")
    public Optional<GUID> getTraceId() {
        return Optional.ofNullable(traceId);
    }

    @SuppressWarnings("unused")
    public boolean getTraceSampled() {
        return traceSampled;
    }

    public void writeHeaderTo(TRequestHeader.Builder header) {
        if (timeout != null) {
            header.setTimeout(RpcUtil.durationToMicros(timeout));
        }
        if (requestId != null) {
            header.setRequestId(RpcUtil.toProto(requestId));
        }
        if (traceId != null) {
            TTracingExt.Builder tracing = TTracingExt.newBuilder();
            tracing.setSampled(traceSampled);
            tracing.setTraceId(RpcUtil.toProto(traceId));
            header.setExtension(TRequestHeader.tracingExt, tracing.build());
        }
        header.setUserAgent(userAgent);
    }

    public final String getArgumentsLogString() {
        StringBuilder sb = new StringBuilder();
        writeArgumentsLogString(sb);

        // trim last space
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    protected void writeArgumentsLogString(StringBuilder sb) {
    }

    public abstract TBuilder toBuilder();

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>, TRequest> {
        protected @Nullable
        Duration timeout;
        protected @Nullable
        GUID requestId;
        protected @Nullable
        GUID traceId;
        protected boolean traceSampled = false;
        protected String userAgent = "yt/java/ytsaurus-client@";

        protected @Nullable
        Message additionalData;

        /**
         * Construct empty builder.
         */
        Builder() {
        }

        protected Builder(Builder<?, ?> other) {
            timeout = other.timeout;
            requestId = other.requestId;
            traceId = other.traceId;
            traceSampled = other.traceSampled;
            userAgent = other.userAgent;
            additionalData = other.additionalData;
        }

        @SuppressWarnings({"unused"})
        public TBuilder setAdditionalData(@Nullable Message additionalData) {
            this.additionalData = additionalData;
            return self();
        }

        protected abstract TBuilder self();

        public abstract TRequest build();

        public TBuilder setTimeout(@Nullable Duration timeout) {
            this.timeout = timeout;
            return self();
        }

        /**
         * Set User-Agent header value
         */
        public TBuilder setUserAgent(String userAgent) {
            this.userAgent = userAgent;
            return self();
        }

        /**
         * Set id of the request.
         *
         * <p> Request id can be used to trace request in YT server logs.
         *
         * <p> Every request must have its own unique request id.
         * If request id is not set or set to null library will generate random request id.
         *
         * @see GUID#create()
         */
        public TBuilder setRequestId(@Nullable GUID requestId) {
            this.requestId = requestId;
            return self();
        }

        /**
         * Set trace id of the request.
         * Sampling is not enabled.
         */
        public TBuilder setTraceId(@Nullable GUID traceId) {
            this.traceId = traceId;
            return self();
        }

        /**
         * Set trace id of the request.
         *
         * @param traceId trace id of the request.
         * @param sampled whether this request will be sent to jaeger.
         *                <b>Warning:</b> enabling sampling creates additional load on server, please be careful.
         */
        public TBuilder setTraceId(@Nullable GUID traceId, boolean sampled) {
            if (sampled && traceId == null) {
                throw new IllegalArgumentException("traceId cannot be null if sampled == true");
            }
            this.traceId = traceId;
            this.traceSampled = sampled;
            return self();
        }

        @SuppressWarnings("unused")
        @Nullable
        Message getAdditionalData() {
            return additionalData;
        }

        public Optional<Duration> getTimeout() {
            return Optional.ofNullable(timeout);
        }

        @SuppressWarnings("unused")
        public Optional<GUID> getRequestId() {
            return Optional.ofNullable(requestId);
        }

        @SuppressWarnings("unused")
        public Optional<GUID> getTraceId() {
            return Optional.ofNullable(traceId);
        }

        @SuppressWarnings("unused")
        public boolean getTraceSampled() {
            return traceSampled;
        }

        public void writeHeaderTo(TRequestHeader.Builder header) {
            if (timeout != null) {
                header.setTimeout(RpcUtil.durationToMicros(timeout));
            }
            if (requestId != null) {
                header.setRequestId(RpcUtil.toProto(requestId));
            }
            if (traceId != null) {
                TTracingExt.Builder tracing = TTracingExt.newBuilder();
                tracing.setSampled(traceSampled);
                tracing.setTraceId(RpcUtil.toProto(traceId));
                header.setExtension(TRequestHeader.tracingExt, tracing.build());
            }
            header.setUserAgent(userAgent);
        }

        public final String getArgumentsLogString() {
            StringBuilder sb = new StringBuilder();
            writeArgumentsLogString(sb);

            // trim last space
            if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ' ') {
                sb.setLength(sb.length() - 1);
            }
            return sb.toString();
        }

        protected void writeArgumentsLogString(StringBuilder sb) {
        }
    }
}
