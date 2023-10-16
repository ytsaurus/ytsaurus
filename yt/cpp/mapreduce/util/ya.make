LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    batch.cpp
    temp_table.cpp
    ypath_join.cpp
    wait_for_tablets_state.cpp
)

GENERATE_ENUM_SERIALIZATION(wait_for_tablets_state.h)

PEERDIR(
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
)

END()

RECURSE_FOR_TESTS(
    ut
)
