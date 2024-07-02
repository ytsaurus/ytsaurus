LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    owning_yamr_row.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
)

END()
