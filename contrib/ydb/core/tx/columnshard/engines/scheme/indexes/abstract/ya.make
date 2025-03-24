LIBRARY()

SRCS(
    constructor.cpp
    collection.cpp
    header.cpp
    fetcher.cpp
    abstract.cpp
    meta.cpp
    checker.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/library/formats/arrow/protos
    yql/essentials/core/arrow_kernels/request
    contrib/ydb/core/formats/arrow/program
)

GENERATE_ENUM_SERIALIZATION(common.h)

YQL_LAST_ABI_VERSION()

END()
