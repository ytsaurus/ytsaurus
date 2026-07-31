LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    error_helpers.cpp
    progress_merger.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson
    yql/essentials/core/issue
    yql/essentials/core/progress_merger
    yql/essentials/providers/common/proto
    yql/essentials/public/issue
    yql/tools/yqlworker/interface/proto
    yql/tools/yqlworker/interface/progress
)

END()
