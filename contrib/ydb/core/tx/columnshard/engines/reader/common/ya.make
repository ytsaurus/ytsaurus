LIBRARY()

SRCS(
    conveyor_task.cpp
    queue.cpp
    description.cpp
    result.cpp
    stats.cpp
)

PEERDIR(
    contrib/ydb/core/tx/program
    contrib/ydb/core/formats/arrow/reader
)

END()
