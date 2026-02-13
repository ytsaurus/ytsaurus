LIBRARY()

SRCS(
    yq_audit_service.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
)

END()

RECURSE(
    events
)
