LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    yt/yt/library/query/base
    yt/yt/library/query/engine
)

END()
