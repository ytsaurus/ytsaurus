LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    publisher.cpp
    sink.cpp
    static_table_export.cpp
    writer_resource.cpp
    writer_storage.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/resources
    yt/yt/library/erasure
)

END()

RECURSE_FOR_TESTS(unittests)
