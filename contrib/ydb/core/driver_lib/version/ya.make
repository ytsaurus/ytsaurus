LIBRARY(version)

SRCS(
    version.cpp
    version.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/actors/interconnect
    library/cpp/monlib/service/pages
    library/cpp/svnversion
    contrib/ydb/core/protos
    contrib/ydb/core/viewer/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
