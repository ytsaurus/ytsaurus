UNITTEST_FOR(contrib/ydb/library/yql/providers/yt/codec/codegen)

SRCS(
    yt_codec_cg_ut.cpp
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()


PEERDIR(
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/providers/yt/codec
)

YQL_LAST_ABI_VERSION()

IF (MKQL_DISABLE_CODEGEN)
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

END()
