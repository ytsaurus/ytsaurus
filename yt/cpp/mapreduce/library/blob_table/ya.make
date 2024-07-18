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

RECURSE_FOR_TESTS(
    ut
    ut_core_http
)
