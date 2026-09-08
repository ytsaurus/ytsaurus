PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/yson/node
    library/cpp/yt/string

    yql/essentials/public/udf/service/exception_policy

    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface

    yt/yql/providers/ytflow/job
    yt/yql/tools/ytflow_worker/config

    yt/yt/client
    yt/yt/core

    yt/yt/flow/library/cpp/pipeline_helpers
    yt/yt/flow/library/cpp/resources
    yt/yt/flow/library/cpp/runner
    yt/yt/flow/library/cpp/connectors/queue

    yt/yt/library/program
)

SRCS(
    ytflow_worker.cpp
)

END()
