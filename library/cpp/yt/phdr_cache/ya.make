LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    phdr_cache.cpp
)

PEERDIR(
    library/cpp/yt/assert
)

END()
