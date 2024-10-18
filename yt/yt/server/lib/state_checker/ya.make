LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    state_checker.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
)

END()
