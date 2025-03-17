LIBRARY()

SRCS(
    abstract.cpp
    context.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
)

END()
