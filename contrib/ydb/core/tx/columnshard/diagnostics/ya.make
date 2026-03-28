LIBRARY()

SRCS(
    scan_diagnostics_actor.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/opentelemetry-proto
    contrib/ydb/core/base/generated
    contrib/ydb/core/control/lib/generated
    contrib/ydb/library/aclib/protos
    contrib/ydb/library/actors/core
    yql/essentials/core/issue/protos
)

RESOURCE(
    viz-global.js viz-global.js
)

END()
