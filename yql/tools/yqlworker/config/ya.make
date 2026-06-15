LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    library/cpp/getopt/small
    library/cpp/logger/global
    library/cpp/protobuf/util
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage
    yql/essentials/utils/log
    yql/essentials/providers/common/proto
    yql/cfg/proto
)

GENERATE_ENUM_SERIALIZATION(config.h)

END()

RECURSE_FOR_TESTS(
    ut
)
