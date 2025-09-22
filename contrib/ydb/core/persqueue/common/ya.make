LIBRARY()

SRCS(
    common_app.cpp
    heartbeat.cpp
    key.cpp
    microseconds_sliding_window.cpp
)

GENERATE_ENUM_SERIALIZATION(sourceid_info.h)

PEERDIR(
    library/cpp/monlib/service/pages
    contrib/ydb/core/persqueue/public
    contrib/ydb/library/actors/core
    contrib/ydb/library/logger
)

END()

RECURSE(
    proxy
)

RECURSE_FOR_TESTS(
    ut
)
