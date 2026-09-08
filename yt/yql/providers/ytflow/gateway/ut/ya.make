GTEST()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/common
    library/cpp/yt/memory

    yql/essentials/minikql/computation/no_llvm
    yql/essentials/providers/common/structured_token
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy

    yt/yql/providers/ytflow/gateway
    yt/yql/providers/ytflow/job
    yt/yt/flow/library/cpp/common
)

SRCS(
    yql_ytflow_pipeline_spec_ut.cpp
    yql_ytflow_utils_ut.cpp
    yql_ytflow_worker_config_ut.cpp
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
