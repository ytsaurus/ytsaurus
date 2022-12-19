package tech.ytsaurus.client.rpc;

import java.util.Optional;

public class RpcCompression {
    private Compression requestCodecId = null;
    private Compression responseCodecId = null;

    public RpcCompression() {
    }

    public RpcCompression(Compression codecId) {
        this(codecId, codecId);
    }

    public RpcCompression(Compression requestCodecId, Compression responseCodecId) {
        this.requestCodecId = requestCodecId;
        this.responseCodecId = responseCodecId;
    }

    public Optional<Compression> getRequestCodecId() {
        return Optional.ofNullable(requestCodecId);
    }

    public Optional<Compression> getResponseCodecId() {
        return Optional.ofNullable(responseCodecId);
    }

    public RpcCompression setRequestCodecId(Compression codecId) {
        this.requestCodecId = codecId;
        return this;
    }

    public RpcCompression setResponseCodecId(Compression codecId) {
        this.responseCodecId = codecId;
        return this;
    }

    public boolean isEmpty() {
        return requestCodecId == null
                && responseCodecId == null;
    }

    @Override
    public String toString() {
        return "RpcCompression{" +
                "requestCodecId=" + requestCodecId +
                ", responseCodecId=" + responseCodecId +
                '}';
    }
}
