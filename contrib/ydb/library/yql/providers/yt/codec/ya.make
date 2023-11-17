LIBRARY()

SRCS(
    yt_codec_io.cpp
    yt_codec_io.h
    yt_codec_job.cpp
    yt_codec_job.h
    yt_codec.cpp
    yt_codec.h
)

PEERDIR(
    library/cpp/streams/brotli
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/io
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/lib/mkql_helpers
    contrib/ydb/library/yql/providers/yt/lib/skiff
)

IF (NOT MKQL_DISABLE_CODEGEN)
    PEERDIR(
        contrib/ydb/library/yql/providers/yt/codec/codegen
    )
ELSE()
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    codegen
)

RECURSE_FOR_TESTS(
    ut
)
