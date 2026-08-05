GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    buffer_state_manager_ut.cpp
    file_storage_ut.cpp
    input_buffer_ut.cpp
    job_tracker_ut.cpp
    message_distributor_ut.cpp
    traced_invoker_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/worker
)

SIZE(SMALL)

END()
