package ru.yandex.yt.ytclient.proxy.request;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.Message;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.tracing.TTracingExt;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

public abstract class RequestBase<T extends RequestBase<T>> {
    private Duration timeout;
    private @Nullable GUID requestId;
    private @Nullable GUID traceId;
    private boolean traceSampled = false;

    Message additionalData;

    protected RequestBase() {
    }

    protected RequestBase(RequestBase<?> other) {
        timeout = other.timeout;
        requestId = other.requestId;
        traceId = other.traceId;
        traceSampled = other.traceSampled;
        additionalData = other.additionalData;
    }

    protected abstract @Nonnull T self();

    @SuppressWarnings("unused")
    Message getAdditionalData() {
        return additionalData;
    }

    @SuppressWarnings({"unused"})
    T setAdditionalData(Message additionalData) {
        this.additionalData = additionalData;
        return self();
    }

    public T setTimeout(@Nullable Duration timeout) {
        this.timeout = timeout;
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
     * @see ru.yandex.inside.yt.kosher.common.GUID#create()
     */
    public T setRequestId(@Nullable GUID requestId) {
        this.requestId = requestId;
        return self();
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

    /**
     * Set trace id of the request.
     * Sampling is not enabled.
     */
    public T setTraceId(@Nullable GUID traceId) {
        this.traceId = traceId;
        return self();
    }

    /**
     * Set trace id of the request.
     *
     * @param traceId trace id of the request.
     * @param sampled whether or not this request will be sent to jaeger.
     *                <b>Warning:</b> enabling sampling creates additional load on server, please be careful.
     */
    public T setTraceId(@Nullable GUID traceId, boolean sampled) {
        if (sampled && traceId == null) {
            throw new IllegalArgumentException("traceId cannot be null if sampled == true");
        }
        this.traceId = traceId;
        this.traceSampled = sampled;
        return self();
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

    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
    }
}
