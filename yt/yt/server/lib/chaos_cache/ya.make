LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    chaos_cache.cpp
    chaos_cache_service.cpp
    config.cpp
)

PEERDIR(
    yt/yt/server/lib/chaos_node
    yt/yt_proto/yt/client
    yt/yt/ytlib
)

END()

RECURSE_FOR_TESTS(
    unittests
)
