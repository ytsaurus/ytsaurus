LIBRARY()

SRCS(
    yql_opt_json_peephole_physical.h
    yql_opt_json_peephole_physical.cpp
    yql_opt_peephole_physical.h
    yql_opt_peephole_physical.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/common_opt
    contrib/ydb/library/yql/core/type_ann
)

YQL_LAST_ABI_VERSION()

END()
