LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(g:yt)

SRCS(
    cache.h
)

PEERDIR(
    library/cpp/testing/gtest_extensions
    yt/yt/client/hedging
)

END()
