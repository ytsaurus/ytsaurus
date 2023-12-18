G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/client
    yt/yt/core
)

TAG(ya:not_autocheck)

END()
