LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    arrow.cpp
    protobuf.h
    protobuf.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface

    yt/yt/library/formats

    contrib/libs/apache/arrow_next
)

END()
