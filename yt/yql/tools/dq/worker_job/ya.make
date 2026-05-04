LIBRARY()

SRCS(
    dq_worker.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/util
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/yt/core
    contrib/ydb/library/yql/dq/actors/spilling
    yql/essentials/providers/common/metrics
    yt/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/runtime
    contrib/ydb/library/yql/providers/dq/service
    contrib/ydb/library/yql/providers/dq/stats_collector
    contrib/ydb/library/yql/providers/dq/task_runner
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/utils
    yql/essentials/utils/network
    yql/essentials/utils/log
    yql/essentials/utils/log/proto
    yt/yql/providers/dq/actors
    yt/yql/providers/dq/actors/yt
    yt/yql/providers/dq/global_worker_manager
    yql/essentials/utils/signals
)

YQL_LAST_ABI_VERSION()

END()
