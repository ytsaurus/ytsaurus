GTEST(unittester-row-merger)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    row_merger_ut.cpp
)

PEERDIR(
    yt/yt/library/row_merger

    yt/yt/library/query/engine

    yt/yt/client
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

END()
