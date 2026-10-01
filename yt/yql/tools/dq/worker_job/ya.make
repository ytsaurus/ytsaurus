LIBRARY()

SRCS(
    dq_worker.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/library/yql/dq/actors/spilling
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/task_runner
    library/cpp/protobuf/util
    yql/essentials/providers/common/metrics
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/utils/log/proto
    yql/essentials/utils/network
    yql/essentials/utils/signals
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/yql/providers/dq/actors
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/global_worker_manager
    yt/yql/providers/dq/runtime
    yt/yql/providers/dq/service
    yt/yql/providers/dq/stats_collector
    yt/yql/tools/dq/job_config
    yt/yt/core
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
