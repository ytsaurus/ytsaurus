LIBRARY()

SRCS(
    check_complete.cpp
    collect_clusters.cpp
    collect_tables.cpp
    collect.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete
    yql/essentials/sql/v1/complete/name/cluster/static
    yql/essentials/sql/v1/complete/name/object/simple/static
    yql/essentials/sql/v1/complete/name/service/cluster
    yql/essentials/sql/v1/complete/name/service/schema
    yql/essentials/sql/v1/complete/name/service/static
    yql/essentials/sql/v1/complete/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/ast
    yql/essentials/core/services
    yql/essentials/core
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/minikql
    yql/essentials/providers/common/provider
    yql/essentials/providers/result/provider
)

END()
