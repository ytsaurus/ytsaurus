LIBRARY()

SRCS(
    stages.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/tx_reader
)

END()
