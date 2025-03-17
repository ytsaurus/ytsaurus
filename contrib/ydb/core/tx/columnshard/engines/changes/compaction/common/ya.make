LIBRARY()

SRCS(
    context.cpp
    result.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme
)

END()
