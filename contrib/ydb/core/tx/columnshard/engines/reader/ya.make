LIBRARY()

SRCS(
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/predicate
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/resources
    contrib/ydb/core/tx/program
    contrib/ydb/core/tx/columnshard/engines/reader/plain_reader
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader
    contrib/ydb/core/tx/columnshard/engines/reader/sys_view
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
    contrib/ydb/core/tx/columnshard/engines/reader/common
    contrib/ydb/core/tx/columnshard/engines/reader/actor
    contrib/ydb/core/tx/columnshard/engines/reader/transaction
    contrib/ydb/core/tx/columnshard/engines/scheme
)

END()
