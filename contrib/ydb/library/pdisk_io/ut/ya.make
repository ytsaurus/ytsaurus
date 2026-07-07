UNITTEST_FOR(contrib/ydb/library/pdisk_io)

IF (OS_LINUX)
    SRCS(
        uring_router_ut.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    contrib/ydb/library/pdisk_io
)

END()
