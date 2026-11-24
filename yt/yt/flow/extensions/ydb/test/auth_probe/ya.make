PROGRAM(ydb_auth_probe)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/json
    yt/yt/core
    yt/yt/flow/extensions/ydb/cpp
    yt/yt/flow/extensions/ydb/sdk
)

END()
