package ru.yandex.yt.ytclient.rpc;

import ru.yandex.yt.ytclient.rpc.internal.Compression;

public class RpcCompression {
    private Compression requestCodecId = null;
    private Compression responseCodecId = null;
    private Compression requestAttachmentsCodecId = null;
    private Compression responseAttachmentsCodecId = null;

    public RpcCompression() { }

    public RpcCompression(Compression codecId) {
        this(codecId, codecId, codecId, codecId);
    }

    public RpcCompression(
            Compression requestCodecId,
            Compression responseCodecId,
            Compression requestAttachmentsCodecId,
            Compression responseAttachmentsCodecId)
    {
        this.requestCodecId = requestCodecId;
        this.responseCodecId = responseCodecId;
        this.requestAttachmentsCodecId = requestAttachmentsCodecId;
        this.responseAttachmentsCodecId = responseAttachmentsCodecId;
    }

    public Compression getRequestCodecId() {
        return requestCodecId;
    }

    public Compression getResponseCodecId() {
        return responseCodecId;
    }

    public Compression getResponseAttachmentsCodecId() {
        return responseAttachmentsCodecId;
    }

    public Compression getRequestAttachmentsCodecId() {
        return requestAttachmentsCodecId;
    }

    public RpcCompression setRequestCodecId(Compression codecId) {
        this.requestCodecId = codecId;
        return this;
    }

    public RpcCompression setResponseCodecId(Compression codecId) {
        this.responseCodecId = codecId;
        return this;
    }

    public RpcCompression setRequestAttachmentsCodecId(Compression codecId) {
        this.requestAttachmentsCodecId = codecId;
        return this;
    }

    public RpcCompression setResponseAttachmentsCodecId(Compression codecId) {
        this.responseAttachmentsCodecId = codecId;
        return this;
    }

    public boolean isEmpty() {
        return requestCodecId == null
                || responseCodecId == null
                || requestAttachmentsCodecId == null
                || responseAttachmentsCodecId == null;
    }
}
