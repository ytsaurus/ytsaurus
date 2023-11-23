LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    alert_manager.cpp
    config.cpp
)

PEERDIR(
    library/cpp/yt/memory

    yt/yt/core
)

END()
