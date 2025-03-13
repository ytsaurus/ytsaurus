LIBRARY()

SRCS(
    interface.cpp
)

PEERDIR(
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/services/bg_tasks/protos
    contrib/ydb/library/conclusion
)

END()
