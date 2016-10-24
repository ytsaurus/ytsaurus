LIBRARY()

OWNER(
    ermolovd
    monster
)

SRCS(
    job_reader.cpp
    job_writer.cpp
    client_reader.cpp
    client_writer.cpp
    block_writer.cpp
    lenval_table_reader.cpp
    node_table_reader.cpp
    node_table_writer.cpp
    yamr_table_reader.cpp
    yamr_table_writer.cpp
    proto_table_reader.cpp
    proto_table_writer.cpp
    proto_helpers.cpp
    file_reader.cpp
    file_writer.cpp
)

PEERDIR(
    library/yson
    mapreduce/yt/http
    mapreduce/yt/interface
    mapreduce/yt/interface/protos
)

END()
