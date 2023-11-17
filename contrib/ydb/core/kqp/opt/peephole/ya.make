LIBRARY()

SRCS(
    kqp_opt_peephole_wide_read.cpp
    kqp_opt_peephole_write_constraint.cpp
    kqp_opt_peephole.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common
    contrib/ydb/library/naming_conventions
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/core/kqp/opt/physical
)

YQL_LAST_ABI_VERSION()

END()
