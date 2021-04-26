package ru.yandex.yt.ytclient.rpc;

public enum RpcErrorCode {
    Ok(0),
    Generic(1),
    Canceled(2),
    Timeout(3),

    ProxyBanned(2100),

    TransportError(100),
    ProtocolError(101),
    NoSuchService(102),
    NoSuchMethod(103),
    Unavailable(105),
    PoisonPill(106),
    RequestQueueSizeLimitExceeded(108),
    AuthenticationError(109),
    InvalidCsrfToken(110),
    InvalidCredentials(111),
    StreamingNotSupported(112),
    TableMountInfoNotReady(1707);

    @SuppressWarnings("checkstyle:VisibilityModifier")
    public final int code;

    RpcErrorCode(int code) {
        this.code = code;
    }

    /**
     * Get value of error code.
     */
    public int getCode() {
        return code;
    }
}
