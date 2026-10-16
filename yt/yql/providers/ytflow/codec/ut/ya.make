UNITTEST_FOR(yt/yql/providers/ytflow/codec)

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/types/uuid

    yt/yt/client
    yt/yt/core
    yt/yt/library/decimal
)

SRCS(
    yql_ytflow_codec_ut.cpp
    yql_ytflow_unboxed_value_setup.cpp
    yql_ytflow_unversioned_row_setup.cpp
)

END()
