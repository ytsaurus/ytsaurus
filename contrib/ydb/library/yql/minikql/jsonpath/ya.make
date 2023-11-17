LIBRARY()

YQL_ABI_VERSION(
    2
    27
    0
)

IF (ARCH_X86_64)
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kHyperscan
    )

    PEERDIR(
        contrib/ydb/library/rewrapper/hyperscan
    )

ELSE()
    CFLAGS(
        -DYDB_REWRAPPER_LIB_ID=kRe2
    )

ENDIF()

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/json
    contrib/ydb/library/rewrapper/re2
    contrib/ydb/library/rewrapper
    contrib/ydb/library/binary_json
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/parser/proto_ast
    contrib/ydb/library/yql/parser/proto_ast/gen/jsonpath
)

SRCS(
    ast_builder.cpp
    ast_nodes.cpp
    binary.cpp
    executor.cpp
    jsonpath.cpp
    parse_double.cpp
    type_check.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(ast_nodes.h)

END()

RECURSE(
    benchmark
)

RECURSE_FOR_TESTS(
    ut
)
