LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    persisted_state_manager.cpp
    worker_tracker.cpp
    yt_connector.cpp
)

PEERDIR(
    library/cpp/testing/gtest_extensions
    yt/yt/flow/library/cpp/controller
)

END()
