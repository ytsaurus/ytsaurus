PY3_LIBRARY()

SET(YT_TESTS_ROOT yt/yt/tests/integration)

PEERDIR(
    ${YT_TESTS_ROOT}/controller
    ${YT_TESTS_ROOT}/cypress
    ${YT_TESTS_ROOT}/dynamic_tables
    ${YT_TESTS_ROOT}/formats
    ${YT_TESTS_ROOT}/master
    ${YT_TESTS_ROOT}/misc
    ${YT_TESTS_ROOT}/node
    ${YT_TESTS_ROOT}/proxies
    ${YT_TESTS_ROOT}/sequoia
    ${YT_TESTS_ROOT}/queues
    ${YT_TESTS_ROOT}/scheduler
    ${YT_TESTS_ROOT}/scheduler_simulator
    ${YT_TESTS_ROOT}/zookeeper
    ${YT_TESTS_ROOT}/queries
)

END()

RECURSE(
    size_xl
    size_l
    size_m
    size_s
    fake_blackbox
    yt_cli
)
    
IF (NOT OPENSOURCE)
    RECURSE(
        compat
    )
ENDIF()

IF (NOT YT_TEAMCITY)
    RECURSE(
        yt_fibers_printer
    )
ENDIF()

RECURSE_FOR_TESTS(
    prepare_scheduling_usage
)
