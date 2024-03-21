LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
)

SRCS(
    blob_table.cpp
)

END()
