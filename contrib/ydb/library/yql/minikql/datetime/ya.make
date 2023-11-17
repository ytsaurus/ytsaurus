LIBRARY()

SRCS(
    datetime.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/computation/llvm
)

YQL_LAST_ABI_VERSION()

END()
