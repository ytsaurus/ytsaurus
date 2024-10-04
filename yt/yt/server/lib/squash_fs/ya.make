LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    squash_fs_layout_builder.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/ytlib
    yt/yt_proto/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
