LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL test.cpp
    GLOBAL schemeshard.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/initiator/status
    contrib/ydb/services/bg_tasks/abstract
)

END()
