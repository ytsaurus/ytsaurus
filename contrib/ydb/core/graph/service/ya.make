LIBRARY()

SRCS(
    log.h
    service_impl.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/graph/api
    contrib/ydb/public/sdk/cpp/src/client/params
)

END()
