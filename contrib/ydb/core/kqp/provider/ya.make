LIBRARY()

SRCS(
    yql_kikimr_datasink.cpp
    yql_kikimr_datasource.cpp
    yql_kikimr_exec.cpp
    yql_kikimr_expr_nodes.h
    yql_kikimr_expr_nodes.cpp
    yql_kikimr_gateway.h
    yql_kikimr_gateway.cpp
    yql_kikimr_opt_build.cpp
    yql_kikimr_opt.cpp
    yql_kikimr_provider.h
    yql_kikimr_provider.cpp
    yql_kikimr_provider_impl.h
    yql_kikimr_results.cpp
    yql_kikimr_results.h
    yql_kikimr_settings.cpp
    yql_kikimr_settings.h
    yql_kikimr_type_ann.cpp
    yql_kikimr_type_ann_pg.h
    yql_kikimr_type_ann_pg.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/docapi
    contrib/ydb/core/kqp/query_data
    contrib/ydb/library/aclib
    contrib/ydb/library/aclib/protos
    contrib/ydb/library/binary_json
    contrib/ydb/library/dynumber
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/core/peephole_opt
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/providers/dq/expr_nodes
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/providers/result/provider
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/ydb_issue/proto
    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

SRCDIR(contrib/ydb/library/yql/core/expr_nodes_gen)

IF(EXPORT_CMAKE)
    RUN_PYTHON3(
        ${ARCADIA_ROOT}/contrib/ydb/library/yql/core/expr_nodes_gen/gen/__main__.py
            yql_expr_nodes_gen.jnj
            yql_kikimr_expr_nodes.json
            yql_kikimr_expr_nodes.gen.h
            yql_kikimr_expr_nodes.decl.inl.h
            yql_kikimr_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_kikimr_expr_nodes.json
        OUT yql_kikimr_expr_nodes.gen.h
        OUT yql_kikimr_expr_nodes.decl.inl.h
        OUT yql_kikimr_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/contrib/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ELSE()
    RUN_PROGRAM(
        contrib/ydb/library/yql/core/expr_nodes_gen/gen
            yql_expr_nodes_gen.jnj
            yql_kikimr_expr_nodes.json
            yql_kikimr_expr_nodes.gen.h
            yql_kikimr_expr_nodes.decl.inl.h
            yql_kikimr_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_kikimr_expr_nodes.json
        OUT yql_kikimr_expr_nodes.gen.h
        OUT yql_kikimr_expr_nodes.decl.inl.h
        OUT yql_kikimr_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/contrib/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ENDIF()

GENERATE_ENUM_SERIALIZATION(yql_kikimr_provider.h)

END()

RECURSE_FOR_TESTS(
    ut
)
