LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    allocation.cpp
    helpers.cpp
    exec_node.cpp
)

PEERDIR(
    yt/yt/server/lib/scheduler
    yt/yt/server/lib/controller_agent

    yt/yt/ytlib

    yt/yt/library/vector_hdrf

    yt/yt/core

    library/cpp/yt/threading
)

END()
