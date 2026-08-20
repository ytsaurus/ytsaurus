LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    plugin.cpp
    helpers.cpp
    task_data_builder_registry.cpp
)

# Task data builders register themselves via static initializers and are not referenced
# by any other translation unit, so their object files must not be dropped at link time.
GLOBAL_SRCS(
    task_data_builder_default.cpp
)

IF (NOT OPENSOURCE)
    GLOBAL_SRCS(
        task_data_builder_yql_service.cpp
    )
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/json
    library/cpp/protobuf/util
    library/cpp/yson
    library/cpp/yt/threading
    yt/yt/core
    yt/yql/plugin
    yt/yql/plugin/lib
    yql/essentials/core/progress_merger
    yql/essentials/providers/common/proto
    yql/essentials/public/issue
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/parser/pg_wrapper
    yql/essentials/utils/log
    yql/tools/yqlworker/interface/msgbus
    yql/tools/yqlworker/proto
)

YQL_LAST_ABI_VERSION()

END()
