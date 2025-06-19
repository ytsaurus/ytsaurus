LIBRARY()

SRCS(
    audit_helper.h
    audit_helper.cpp
)

PEERDIR(
    library/cpp/logger
    contrib/ydb/core/audit
)

END()
