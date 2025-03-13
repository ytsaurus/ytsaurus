LIBRARY()

SRCS(
    kqp_opt_phy_build_stage.cpp
    kqp_opt_phy_limit.cpp
    kqp_opt_phy_olap_agg.cpp
    kqp_opt_phy_olap_filter.cpp
    kqp_opt_phy_precompute.cpp
    kqp_opt_phy_sort.cpp
    kqp_opt_phy_source.cpp
    kqp_opt_phy_helpers.cpp
    kqp_opt_phy_stage_float_up.cpp
    kqp_opt_phy.cpp
    predicate_collector.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/opt/physical/effects
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/type_ann
)

YQL_LAST_ABI_VERSION()

END()
