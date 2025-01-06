LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core

    yt/yt_proto/yt/orm/client/proto
)

SRCS(
    protobuf_helpers.cpp
    public.cpp
    traits.cpp
)

END()
