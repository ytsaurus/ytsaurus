LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    chunk/config.cpp
    dynamic_table/config.cpp
    image/config.cpp
    journal/config.cpp
    memory/config.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
)

END()
