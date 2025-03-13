LIBRARY()

SRCS(
    granule.cpp
    storage.cpp
    portions_index.cpp
    stages.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    contrib/ydb/core/tx/columnshard/engines/storage/actualizer/index
    contrib/ydb/core/tx/columnshard/counters
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/base
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/optimizer
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner
)

GENERATE_ENUM_SERIALIZATION(granule.h)

END()
