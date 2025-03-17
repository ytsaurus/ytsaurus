LIBRARY()

SRCS(
    scheme.cpp
    counters.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
)

END()
