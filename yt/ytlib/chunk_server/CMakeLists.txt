SET(YT_SOURCE_FILES
    common.cpp

    chunk_holder.proto
    chunk.proto
    chunk_service_rpc.proto
    file_chunk_server_meta.proto
    table_chunk_meta.proto

    chunk_service_rpc.h
    chunk_service.cpp

    holder.cpp
    chunk.cpp
    chunk_list.cpp
    job.cpp
    job_list.cpp

    chunk_manager.proto
    chunk_manager.cpp

    chunk_placement.cpp

    chunk_replication.cpp

    holder_lease_tracker.cpp

    holder_authority.cpp

    cypress_integration.cpp
)
