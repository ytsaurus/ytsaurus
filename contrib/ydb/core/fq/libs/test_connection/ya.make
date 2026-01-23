LIBRARY()

SRCS(
    counters.cpp
    probes.cpp
    test_connection.cpp
    test_data_streams.cpp
    test_monitoring.cpp
    test_object_storage.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/xml/document
    contrib/ydb/core/fq/libs/actors
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/control_plane_storage
    contrib/ydb/core/fq/libs/test_connection/events
    contrib/ydb/library/yql/providers/pq/cm_client
    contrib/ydb/library/yql/providers/solomon/actors
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
