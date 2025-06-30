UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    channel_scheduler_ut.cpp
    connection_checker_ut.cpp
    event_holder_pool_ut.cpp
    interconnect_ut.cpp
    large.cpp
    outgoing_stream_ut.cpp
    poller_actor_ut.cpp
    dynamic_proxy_ut.cpp
    sticking_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/ut/lib
    contrib/ydb/library/actors/interconnect/ut/protos
    contrib/ydb/library/actors/testlib
    library/cpp/digest/md5
    library/cpp/testing/unittest
)

END()
