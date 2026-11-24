PROGRAM(ydb_fixture_probe)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/json
    yt/yt/core
    yt/yt/flow/extensions/ydb/sdk
    yt/yt/library/arcadia_future_interop
)

END()
