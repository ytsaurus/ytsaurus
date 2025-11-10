LIBRARY()

SRCS(
    abstract.cpp
    counters.cpp
    one_layer.cpp
    zero_level.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
)

END()
