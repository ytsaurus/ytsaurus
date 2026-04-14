LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    kv.cpp
)

PEERDIR(
    contrib/ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(kv.h)

END()
