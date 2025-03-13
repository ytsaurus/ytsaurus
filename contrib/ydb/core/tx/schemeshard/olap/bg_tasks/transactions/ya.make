LIBRARY()

SRCS(
    tasks_list.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/bg_tasks/abstract
    contrib/ydb/core/tx/schemeshard/olap/bg_tasks/protos
)

YQL_LAST_ABI_VERSION()

END()
