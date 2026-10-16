LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    computation.cpp
    computation_runtime_context.cpp
    runtime_init_context.cpp
    source_computation.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/process_function
    yt/yt/flow/library/cpp/computation
)

END()
