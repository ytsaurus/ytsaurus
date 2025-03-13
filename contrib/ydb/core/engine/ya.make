LIBRARY()

SRCS(
    kikimr_program_builder.cpp
    mkql_engine_flat.cpp
    mkql_engine_flat_extfunc.cpp
    mkql_engine_flat_host.cpp
    mkql_keys.cpp
    mkql_proto.cpp
    mkql_proto.h
)

PEERDIR(
    library/cpp/containers/stack_vector
    library/cpp/deprecated/enum_codegen
    library/cpp/random_provider
    library/cpp/time_provider
    contrib/ydb/core/base
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/library/mkql_proto
    contrib/ydb/library/mkql_proto/protos
#    ydb/library/mkql_proto/ut/helpers
    contrib/ydb/public/api/protos
    yql/essentials/minikql/comp_nodes
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/decimal
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    minikql
)

RECURSE_FOR_TESTS(
    ut
)
