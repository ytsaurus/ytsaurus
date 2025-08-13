LIBRARY()

SRCS(
    GLOBAL sub_columns_fetching.cpp
    GLOBAL default_fetching.cpp
    constructor.cpp
    context.cpp
    fetch_steps.cpp
    fetched_data.cpp
    fetching.cpp
    iterator.cpp
    source.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/formats/arrow/accessor/sub_columns
    yql/essentials/minikql
    contrib/ydb/core/util/evlog
)

GENERATE_ENUM_SERIALIZATION(source.h)

END()
