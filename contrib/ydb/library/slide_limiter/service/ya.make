LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    contrib/ydb/library/slide_limiter/usage
    contrib/ydb/core/protos
)

END()
