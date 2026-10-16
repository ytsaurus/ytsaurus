LIBRARY()

SRCS(
    worker_api_msgbus.cpp
)

PEERDIR(
    yql/tools/yqlworker/interface
    yql/tools/yqlworker/proto
    yql/tools/yqlworker/rpc
    yql/tools/yqlworker/misc

    yql/essentials/utils/log
    yql/essentials/public/issue
    yql/essentials/public/langver
    yql/essentials/utils

    library/cpp/messagebus
)

END()
