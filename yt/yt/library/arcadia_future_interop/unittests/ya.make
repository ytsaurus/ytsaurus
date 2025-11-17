GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    interop_ut.cpp
)

PEERDIR(
    yt/yt/library/arcadia_future_interop
)

SIZE(SMALL)

END()
