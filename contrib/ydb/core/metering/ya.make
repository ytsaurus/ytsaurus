RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    bill_record.cpp
    bill_record.h
    metering.cpp
    metering.h
    stream_ru_calculator.cpp
    time_grid.h
)

GENERATE_ENUM_SERIALIZATION(bill_record.h)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/json
    library/cpp/logger
    contrib/ydb/core/base
)

END()
