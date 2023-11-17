IF (OS_LINUX OR OS_DARWIN)
    UNITTEST_FOR(contrib/ydb/library/yql/utils/actors)

    SIZE(SMALL)

    SRCS(
        http_sender_actor_ut.cpp
    )

    PEERDIR(
        contrib/ydb/core/testlib/basics/default
    )

    END()
ENDIF()
