LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    computation.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/computation
)

END()
