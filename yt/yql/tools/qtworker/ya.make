PROGRAM(qtworker)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

ALLOCATOR(J)

SRCS(
    main.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    yql/tools/yqlworker/executor/common/tasks
    yql/tools/yqlworker/core
    yql/tools/yqlworker/lib
    yql/tools/yqlworker/yt_runtime
)

IF (OPENSOURCE)
    CHECK_DEPENDENT_DIRS(ALLOW_ONLY PEERDIRS
        build
        certs
        contrib/deprecated
        contrib/libs
        contrib/proto
        contrib/restricted
        contrib/ydb/core
        contrib/ydb/library
        contrib/ydb/public
        library
        tools
        util
        yql/cfg/proto
        yql/essentials
        yql/providers/dq/metrics
        yql/providers/stat/expr_nodes
        yql/tools/yqlworker
        yt/cpp
        yt/yql
        yt/yt
        yt/yt_proto
    )
ENDIF()

YQL_CURRENT_ABI_VERSION()

END()
