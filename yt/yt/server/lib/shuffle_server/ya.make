LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    shuffle_service.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
