PROGRAM(mrjob)

ALLOCATOR(J)

SRCS(
    mrjob.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    yt/cpp/mapreduce/client
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/providers/yt/job
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    test
)
