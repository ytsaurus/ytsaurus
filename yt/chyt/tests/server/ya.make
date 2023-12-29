PY3TEST()

SET(YT_SPLIT_FACTOR 30)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

PEERDIR(
    yt/yt/tests/conftest_lib
    yt/yt/tests/library
    yt/python/yt/clickhouse
)

DEPENDS(
    yt/chyt/server/bin
    yt/chyt/trampoline
    yt/yt/server/log_tailer/bin
    yt/chyt/tests/dummy_logger
)

DEPENDS(yt/yt/packages/tests_package)

TEST_SRCS(
    conftest.py
    base.py
    helpers.py
    test_atomicity.py
    test_clickhouse_schema.py
    test_columnar_read.py
    test_common.py
    test_composite.py
    test_ddl.py
    test_dynamic_tables.py
    test_http_proxy.py
    test_input_fetching.py
    test_join_and_in.py
    test_log_tailer.py
    test_mutations.py
    test_prewhere.py
    test_query_log.py
    test_query_registry.py
    test_query_tracker.py
    test_sql_udf.py
    test_statistics_tables.py
    test_table_functions.py
    test_tracing.py
    test_version_functions.py
    test_yson_functions.py
    test_yt_dictionaries.py
)

END()

