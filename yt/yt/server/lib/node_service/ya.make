LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    yt/yt/ytlib

    yt/yt/client
)

END()
