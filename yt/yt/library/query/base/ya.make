LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    ast.cpp
    ast_visitors.cpp
    constraints.cpp
    coordination_helpers.cpp
    functions.cpp
    helpers.cpp
    builtin_function_registry.cpp
    builtin_function_types.cpp
    functions_common.cpp
    key_trie.cpp
    lexer.rl6
    parser.ypp
    public.cpp
    push_down_group_by.cpp
    query.cpp
    query_common.cpp
    query_helpers.cpp
    query_preparer.cpp
    query_visitors.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/query/misc
    yt/yt/library/query/proto
)

END()
