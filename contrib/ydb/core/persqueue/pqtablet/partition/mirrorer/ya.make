LIBRARY()

SRCS(
    mirrorer.cpp
)



PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/persqueue/common/proxy
    contrib/ydb/core/persqueue/pqtablet/common
    contrib/ydb/core/persqueue/public/write_meta
)

END()

