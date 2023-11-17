LIBRARY()

SRCS(
    registry.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/minikql/computation/llvm
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
