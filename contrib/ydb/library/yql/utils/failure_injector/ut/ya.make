IF (OS_LINUX OR OS_DARWIN)
    UNITTEST_FOR(contrib/ydb/library/yql/utils/failure_injector)

    SIZE(SMALL)

    SRCS(
        failure_injector_ut.cpp
    )

    PEERDIR(
        contrib/ydb/library/yql/utils/log
    )

    END()
ENDIF()
