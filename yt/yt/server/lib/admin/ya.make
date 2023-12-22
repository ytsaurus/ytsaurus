LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    admin_service.cpp
    restart_service.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/library/coredumper
)

END()
