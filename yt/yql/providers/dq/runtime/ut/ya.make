UNITTEST_FOR(yt/yql/providers/dq/runtime)

SRCS(
    file_cache_ut.cpp
)

IF (NOT OS_WINDOWS)
    PEERDIR(
        yql/essentials/public/udf/service/stub
        yql/essentials/sql/pg_dummy
    )

    SRCS(
        porto_ut.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
