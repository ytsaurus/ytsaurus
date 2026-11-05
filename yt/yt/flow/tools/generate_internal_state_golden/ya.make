PROGRAM(generate_internal_state_golden)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(main.cpp)

PEERDIR(
    yt/yt/core
    yt/yt/flow/library/cpp/delta_codecs
)

END()
