LIBRARY()

SRCS(
    GLOBAL dictionary_fetching.cpp
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
    contrib/ydb/core/formats/arrow/accessor/dictionary
    contrib/ydb/core/formats/arrow/accessor/plain
    contrib/ydb/core/formats/arrow/accessor/sub_columns
    contrib/ydb/core/tx/columnshard/engines/reader/tracing
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/tx/columnshard/engines/storage/indexes/skip_index
    contrib/ydb/core/util/evlog
    yql/essentials/minikql
)

GENERATE_ENUM_SERIALIZATION(source.h)
YQL_LAST_ABI_VERSION()

END()
