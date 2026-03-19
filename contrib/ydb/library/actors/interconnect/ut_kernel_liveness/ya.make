UNITTEST()

IF (OS_LINUX)

SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

SRCS(
    tcp_user_timeout_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/mock
    contrib/ydb/library/actors/interconnect/ut/lib
    contrib/ydb/library/actors/interconnect/ut/lib/port_manager
    contrib/ydb/library/actors/interconnect/ut/protos
    library/cpp/testing/unittest
)

ENDIF()

END()
