LIBRARY()

SRCS(
    write.cpp
    write_data.cpp
    slice_builder.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/data_events
    contrib/ydb/services/metadata
)

END()
