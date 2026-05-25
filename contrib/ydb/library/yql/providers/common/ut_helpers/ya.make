LIBRARY()

SRCS(
    dq_fake_ca.cpp
)

PEERDIR(
    library/cpp/retry
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/services
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/actors/protos
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
)

YQL_LAST_ABI_VERSION()

END()
