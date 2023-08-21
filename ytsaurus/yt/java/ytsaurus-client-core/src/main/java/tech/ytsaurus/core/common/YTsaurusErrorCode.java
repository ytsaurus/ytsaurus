package tech.ytsaurus.core.common;

public enum YTsaurusErrorCode {
    Ok(0),
    Generic(1),
    Canceled(2),
    Timeout(3),

    TooManyRequests(429),

    ProxyBanned(2100),

    TransportError(100),
    ProtocolError(101),
    NoSuchService(102),
    NoSuchMethod(103),
    Unavailable(105),
    PoisonPill(106),
    RpcRequestQueueSizeLimitExceeded(108),
    AuthenticationError(109),
    InvalidCsrfToken(110),
    InvalidCredentials(111),
    StreamingNotSupported(112),
    PeerBanned(115),

    TooManyOperations(202),

    SessionAlreadyExists(703),
    ChunkAlreadyExists(704),
    WindowError(705),
    BlockContentMismatch(706),
    InvalidBlockChecksum(721),
    BlockOutOfRange(722),
    MissingExtension(724),
    NoSuchBlock(707),
    NoSuchChunk(708),
    NoSuchChunkList(717),
    NoSuchChunkTree(713),
    NoSuchChunkView(727),
    NoSuchMedium(719),

    RequestQueueSizeLimitExceeded(904),

    TransactionLockConflict(1700),
    AllWritesDisabled(1703),
    TableMountInfoNotReady(1707),

    OperationProgressOutdated(1911),

    NoSuchTransaction(11000);

    @SuppressWarnings("checkstyle:VisibilityModifier")
    public final int code;

    YTsaurusErrorCode(int code) {
        this.code = code;
    }

    /**
     * Get value of error code.
     */
    public int getCode() {
        return code;
    }
}
