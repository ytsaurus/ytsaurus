LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/util
    contrib/ydb/library/aclib/protos
)

SRCS(
    aclib.cpp
    aclib.h
)

END()

RECURSE_FOR_TESTS(
    ut
    benchmark
)
