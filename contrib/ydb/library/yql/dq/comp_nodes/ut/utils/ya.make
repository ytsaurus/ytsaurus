LIBRARY()


SRCS(
    utils.cpp
    dq_factories.cpp
)
PEERDIR(
    contrib/ydb/library/yql/dq/comp_nodes
    yql/essentials/minikql/comp_nodes/test_support/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)
YQL_LAST_ABI_VERSION()

END()
