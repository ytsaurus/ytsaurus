LIBRARY()

SRCS(
    kqp_opt_log_effects.cpp
    kqp_opt_log_extract.cpp
    kqp_opt_log_helpers.cpp
    kqp_opt_log_join.cpp
    kqp_opt_log_indexes.cpp
    kqp_opt_log_ranges.cpp
    kqp_opt_log_ranges_predext.cpp
    kqp_opt_log_sort.cpp
    kqp_opt_log_sqlin.cpp
    kqp_opt_log_sqlin_compact.cpp
    kqp_opt_log.cpp
    kqp_opt_cbo.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common
    yql/essentials/core/extract_predicate
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/opt
)

YQL_LAST_ABI_VERSION()

END()
