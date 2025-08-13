LIBRARY()

SRCS(
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/portions
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/chunks
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/granules
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/optimizer
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/schemas
)

END()
