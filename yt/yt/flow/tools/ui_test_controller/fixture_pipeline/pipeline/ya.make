PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/runner
    yt/yt/flow/tools/ui_test_controller/fixture_pipeline/lib
)

END()
