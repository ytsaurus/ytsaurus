LIBRARY()

SRCS(
    abstract.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/services/bg_tasks/abstract
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
