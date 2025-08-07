LIBRARY()

PEERDIR(
    library/cpp/json
    contrib/ydb/core/base
    contrib/ydb/core/change_exchange
    contrib/ydb/core/fq/libs/row_dispatcher/events
    contrib/ydb/core/io_formats/cell_maker
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/scheme_types
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/replication/common
    contrib/ydb/core/tx/replication/ydb_proxy
    contrib/ydb/core/tx/replication/ydb_proxy/local_proxy
    contrib/ydb/core/wrappers
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
)

SRCS(
    base_table_writer.cpp
    json_change_record.cpp
    service.cpp
    table_writer.cpp
    topic_reader.cpp
    worker.cpp
)

GENERATE_ENUM_SERIALIZATION(worker.h)

YQL_LAST_ABI_VERSION()

IF (!OS_WINDOWS)
    SRCS(
        s3_writer.cpp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut_json_change_record
    ut_table_writer
    ut_topic_reader
    ut_worker
)

IF (!OS_WINDOWS)
    RECURSE_FOR_TESTS(
        ut_s3_writer
    )
ENDIF()
