LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    plugin.cpp
    helpers.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/json
    library/cpp/protobuf/util
    library/cpp/yson
    library/cpp/yson/node
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
)

YQL_LAST_ABI_VERSION()

END()
