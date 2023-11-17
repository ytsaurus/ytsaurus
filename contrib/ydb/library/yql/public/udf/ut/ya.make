UNITTEST_FOR(contrib/ydb/library/yql/public/udf)

SRCS(
    udf_counter_ut.cpp
    udf_value_ut.cpp
    udf_value_builder_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
