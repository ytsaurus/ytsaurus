LIBRARY()

SRCS(
    yql_exec.cpp
    yql_lazy_init.cpp
    yql_optimize.cpp
    yql_visit.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
)

END()
