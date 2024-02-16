RECURSE(
    packages/yt_setup

    yt

    yt/environment/bin/yt_env_watcher_make
    yt/environment/bin/init_operations_archive
    yt/environment/bin/init_queue_agent_state
    yt/environment/bin/init_query_tracker_state

    yt/local
    yt/local/bin/yt_local_make

    yt/operations_archive

    yt/wrapper
    yt/wrapper/testlib
    yt/wrapper/bin

    yt/cli

    client
    client_lite
    client_with_rpc
)

RECURSE_FOR_TESTS(
    yt/local/tests
    yt/skiff/tests
    yt/yson/tests
    yt/wrapper/tests
    yt/environment/migrationlib/tests
)

IF(NOT OS_DARWIN)
    RECURSE_FOR_TESTS(
        yt/clickhouse/tests
    )
ENDIF()

IF (NOT OPENSOURCE)
    RECURSE(
        # TODO: RECURSE uncoditionally after improving examples/ for opensource.
        examples
        yandex_examples

        yt/infra_api

        yt/transfer_manager
        yt/transfer_manager/client/bin

        yt/scheduler_tools

        yt/tools
        yt/tools/bin

        yt/cpp_wrapper

        yt/flask_helpers

        yt/wrapper/benchmarks
        yt/yson/benchmarks

        # It is actually used only when we build ORM package.
        contrib/prettytable

        experiments
    )

    RECURSE_FOR_TESTS(
        yt/cpp_wrapper/tests
    )
ENDIF()
