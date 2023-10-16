LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    helpers.cpp
    timestamp_proxy_service.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
