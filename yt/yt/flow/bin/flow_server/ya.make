PROGRAM()

SET(FLOW_CMAKE_TARGET_NAME flow_server)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

IF (NOT OPENSOURCE)
    PEERDIR(
        yt/yt/flow/yandex/flow_server_dependencies
    )
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/connectors/servicelog
    yt/yt/flow/library/cpp/connectors/sorted_dynamic_table
    yt/yt/flow/library/cpp/connectors/static_table
    yt/yt/flow/library/cpp/resources
    yt/yt/flow/library/cpp/runner
)

END()
