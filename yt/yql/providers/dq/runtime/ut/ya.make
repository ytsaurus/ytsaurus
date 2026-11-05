UNITTEST_FOR(yt/yql/providers/dq/runtime)

SRCS(
    file_cache_ut.cpp
)

IF (NOT OS_WINDOWS)
    PEERDIR(
        yql/essentials/public/udf/service/stub
    )

    SRCS(
        porto_ut.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
