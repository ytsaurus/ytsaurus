LIBRARY()

PEERDIR(
    yql/essentials/minikql
    yql/essentials/utils
    yql/essentials/public/udf
    contrib/ydb/core/formats/arrow/hash
    contrib/ydb/core/tx/schemeshard/olap/schema
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/formats
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    sharding.cpp
    hash.cpp
    unboxed_reader.cpp
    hash_slider.cpp
    GLOBAL hash_modulo.cpp
    GLOBAL hash_intervals.cpp
    random.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_WINDOWS
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)