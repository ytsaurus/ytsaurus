LIBRARY()

PEERDIR(
    yt/yt/core
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/runner/no_engine
)

SRCS(
    ytflow_worker_config.cpp
)

END()
