LIBRARY()

SRCS(
    tiering.cpp
    counters.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
)

END()
