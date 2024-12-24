LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL handlers.cpp
)

PEERDIR(
    library/cpp/yt/exception
    library/cpp/yt/string
)

END()
