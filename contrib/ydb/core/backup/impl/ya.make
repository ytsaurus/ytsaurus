LIBRARY()

SRCS(
    local_partition_reader.cpp
    table_writer.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/tx/replication/service
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut_local_partition_reader
    ut_table_writer
)
