PY3_LIBRARY()

PEERDIR(
    yt/python/yt
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
    yt/python/client_with_rpc
    contrib/python/numpy
    contrib/python/ipython
)

NO_LINT()

PY_SRCS(
    NAMESPACE lib

    __init__.py
    aggregate.py
    create_data.py
    group_by.py
    helpers.py
    job_base.py
    logger.py
    lookup.py
    mapreduce.py
    parser.py
    parser_impl.py
    process_runner.py
    reshard.py
    replication_helpers.py
    runner.py
    schema.py
    select.py
    spec.py
    stateless_write.py
    table_creation.py
    test_compare_replicas.py
    test_ordered.py
    test_sorted.py
    verify.py
    write_data.py

    ql_base.py
    ql_checks.py
    ql_engine.py
    ql_generator.py
    ql_printer.py
    ql_util.py
)

END()
