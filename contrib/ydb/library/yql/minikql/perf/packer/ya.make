PROGRAM()

ALLOCATOR(J)

PEERDIR(
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    packer.cpp
)

END()
