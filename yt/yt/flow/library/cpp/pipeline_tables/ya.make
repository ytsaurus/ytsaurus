LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    definitions.cpp
)

RESOURCE_FILES(
    PREFIX yt/yt/flow/library/pipeline_tables/
    STRIP ../../pipeline_tables/
    ../../pipeline_tables/definitions.yson
)

PEERDIR(
    library/cpp/resource
    yt/yt/client
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
