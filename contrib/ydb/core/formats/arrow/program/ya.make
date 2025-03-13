LIBRARY()

PEERDIR(
    contrib/ydb/library/conclusion
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
    contrib/ydb/core/formats/arrow/accessor/sub_columns
)

IF (OS_WINDOWS)
    ADDINCL(
        contrib/ydb/library/yql/udfs/common/clickhouse/client/base
        contrib/ydb/library/arrow_clickhouse
    )
ELSE()
    PEERDIR(
        contrib/ydb/library/arrow_clickhouse
    )
    ADDINCL(
        contrib/ydb/library/arrow_clickhouse
    )
ENDIF()

SRCS(
    abstract.cpp
    graph.cpp
    original.cpp
    collection.cpp
    functions.cpp
    aggr_keys.cpp
    aggr_common.cpp
    filter.cpp
    projection.cpp
    assign_const.cpp
    assign_internal.cpp
    chain.cpp
    custom_registry.cpp
    GLOBAL kernel_logic.cpp
)

GENERATE_ENUM_SERIALIZATION(abstract.h)
GENERATE_ENUM_SERIALIZATION(aggr_common.h)

YQL_LAST_ABI_VERSION()

END()
