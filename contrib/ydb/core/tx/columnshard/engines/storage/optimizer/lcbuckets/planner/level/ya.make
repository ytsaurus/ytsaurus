LIBRARY()

SRCS(
    abstract.cpp
    zero_level.cpp
    common_level.cpp
    counters.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
)

END()
