PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    yt/yt/core
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/controller
    yt/yt/flow/library/cpp/runner
    yt/yt/flow/tools/ui_test_controller/fixture_pipeline/lib
    yt/yt/library/program
)

END()

RECURSE_FOR_TESTS(
    fixture_pipeline
    tests
)
