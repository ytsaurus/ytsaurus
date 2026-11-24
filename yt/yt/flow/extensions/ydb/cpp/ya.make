LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    attributes.cpp
    credentials.cpp
    deferred_publication.cpp
    driver_core.cpp
    helpers.cpp
    options.cpp
    payload_splitter.cpp
    sink.cpp
    sink_engine.cpp
    source.cpp
    source_engine.cpp
    spec.cpp
    state.cpp
    topic_info.cpp
    two_phase_commit_engine.cpp
    two_phase_commit_sink.cpp
    writer.cpp
    ydb_topic_driver.cpp
    GLOBAL register.cpp
)

PEERDIR(
    library/cpp/yt/logging/backends/arcadia
    library/cpp/framing
    yt/yt/flow/extensions/framing/cpp
    yt/yt/flow/extensions/ydb/sdk
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/library/arcadia_future_interop
)

END()


RECURSE_FOR_TESTS(
    unittests
)
