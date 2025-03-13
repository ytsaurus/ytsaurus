LIBRARY()

SRCS(
    scalar.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/defaults/protos
    contrib/libs/apache/arrow
    contrib/ydb/library/conclusion
    contrib/ydb/core/scheme_types
    contrib/ydb/library/actors/core
)

END()
