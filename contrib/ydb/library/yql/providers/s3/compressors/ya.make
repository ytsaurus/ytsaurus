LIBRARY()
PEERDIR(
    contrib/libs/fmt
    contrib/libs/poco/Util
    contrib/libs/brotli/dec
    contrib/libs/libbz2
    contrib/libs/lz4
    contrib/libs/lzma
    contrib/libs/zstd
    contrib/ydb/library/yql/udfs/common/clickhouse/client
)

ADDINCL(
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base
    contrib/ydb/library/yql/udfs/common/clickhouse/client/base/pcg-random
    contrib/ydb/library/yql/udfs/common/clickhouse/client/src
)

IF (CLANG AND NOT WITH_VALGRIND)
    SRCS(
        brotli.cpp
        bzip2.cpp
        gz.cpp
        factory.cpp
        lz4io.cpp
        zstd.cpp
        xz.cpp
    )
ELSE()
    SRCS(
        factory.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

