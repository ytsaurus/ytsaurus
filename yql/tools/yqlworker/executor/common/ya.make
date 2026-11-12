YQL_LIBRARY()

SRCS(
    credentials.cpp
    sql_library_meta_task.cpp
    task_progress_merger.cpp
    udf_library_meta_task.cpp
    udf_snapshot_handler_task.cpp
    runnable_tasks.cpp
    task.cpp
)

PEERDIR(
    yql/essentials/core/credentials
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/url_preprocessing/interface
    yql/essentials/core/progress_merger
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/utils/signals
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/udf_resolve
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/gateway/profile
    yt/yql/providers/yt/provider
    yql/tools/yqlworker/config
    yql/tools/yqlworker/misc
    yql/tools/yqlworker/proto
    yql/tools/yqlworker/interface/proto
    yql/tools/yqlworker/interface/progress
    yt/yql/providers/dq/control
    contrib/ydb/library/yql/providers/dq/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
