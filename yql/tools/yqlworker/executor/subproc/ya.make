YQL_LIBRARY()

SRCS(
    control_protocol.cpp
    control_protocol.proto
    data_protocol.cpp
    data_protocol.proto
    task_index.cpp
)

PEERDIR(
    library/cpp/messagebus
    library/cpp/messagebus/protobuf
    library/cpp/protobuf/util
    library/cpp/sighandler
    library/cpp/string_utils/parse_size
    library/cpp/threading/future
    yql/essentials/utils/log
    yql/essentials/utils/signals
    yql/essentials/providers/common/metrics
    yql/tools/yqlworker/config
    yql/tools/yqlworker/executor/common
    yql/tools/yqlworker/logging
    yql/tools/yqlworker/misc
    yql/tools/yqlworker/solomon_agent
    yql/tools/yqlworker/yt_runtime
    yql/tools/yqlworker/interface/proto
)

IF (OS_LINUX OR OS_DARWIN)
    SRCS(
        bottom_half.cpp
        top_half.cpp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
