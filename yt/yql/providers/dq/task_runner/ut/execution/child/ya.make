PROGRAM(pipe_smoke_child)

NO_BUILD_IF(OS_WINDOWS)

PEERDIR(
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/computation
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yt/yql/providers/dq/runtime
)

SRCS(
    main.cpp
)

YQL_LAST_ABI_VERSION()

END()
