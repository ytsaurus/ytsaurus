PROGRAM()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/minikql/arrow
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
	library/cpp/getopt
)

SRCS(
    block_groupby.cpp
)

YQL_LAST_ABI_VERSION()

END()
