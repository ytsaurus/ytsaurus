GTEST(unittester-yql-plugin)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config_ut.cpp
    progress_merger_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yt/yql/plugin
    yt/yql/plugin/lib
    yt/yt/core/test_framework
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
