LIBRARY()

SRCS(
    yql_dq_integration_impl.cpp
    yql_dq_optimization_impl.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/integration
)

END()
