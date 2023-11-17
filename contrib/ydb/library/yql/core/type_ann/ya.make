LIBRARY()

SRCS(
    type_ann_blocks.cpp
    type_ann_blocks.h
    type_ann_columnorder.cpp
    type_ann_columnorder.h
    type_ann_core.cpp
    type_ann_core.h
    type_ann_expr.cpp
    type_ann_expr.h
    type_ann_impl.h
    type_ann_join.cpp
    type_ann_list.cpp
    type_ann_list.h
    type_ann_pg.cpp
    type_ann_pg.h
    type_ann_types.cpp
    type_ann_types.h
    type_ann_wide.cpp
    type_ann_wide.h
    type_ann_match_recognize.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/expr_nodes
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/providers/common/schema/expr
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/parser/pg_wrapper/interface
)

YQL_LAST_ABI_VERSION()

END()
