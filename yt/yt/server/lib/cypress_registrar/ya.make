LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    cypress_registrar.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
