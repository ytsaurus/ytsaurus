YQL_LIBRARY(yqlworker)

SRCS(
    yqlworker.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/provider
    library/cpp/getopt
    library/cpp/svnversion
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/utils/network
    yql/essentials/utils/sys
    yql/tools/yqlworker/config
    yql/tools/yqlworker/dq
    yql/tools/yqlworker/logging
    yql/tools/yqlworker/misc
    yql/tools/yqlworker/solomon_agent
    yql/tools/yqlworker/yt_runtime
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
)

END()
