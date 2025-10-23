LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    nested_row_merger.cpp
    overlapping_reader.cpp
    row_merger.cpp
    versioned_row_merger.cpp
)

PEERDIR(
    yt/yt/client

    yt/yt/library/numeric
    yt/yt/library/query/base
    yt/yt/library/query/engine_api

    yt/yt/core

    library/cpp/yt/compact_containers
    library/cpp/yt/memory
    library/cpp/yt/yson_string
)

END()

RECURSE_FOR_TESTS(
    unittests
)
