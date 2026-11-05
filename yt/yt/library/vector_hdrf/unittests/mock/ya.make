LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    fair_share_update_mock.cpp
)

PEERDIR(
    yt/yt/library/vector_hdrf
)

END()
