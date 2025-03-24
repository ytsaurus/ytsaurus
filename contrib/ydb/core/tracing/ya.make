LIBRARY()

SRCS(
    http.cpp
    tablet_info.cpp
    trace.cpp
    trace_collection.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/mon
)

END()
