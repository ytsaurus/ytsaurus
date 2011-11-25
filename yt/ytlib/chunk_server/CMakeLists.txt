SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_service_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_service.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/holder.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_list.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/job.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/job_list.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_manager.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_placement.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_replication.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/holder_expiration.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/cypress_integration.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_service_rpc.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server/chunk_manager.proto YT_SOURCE_FILES )
