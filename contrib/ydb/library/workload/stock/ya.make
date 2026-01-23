LIBRARY()

SRCS(
    GLOBAL registrar.cpp
    stock.cpp
)

PEERDIR(
    contrib/ydb/library/workload/abstract
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(stock.h)

END()
