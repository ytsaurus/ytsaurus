SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/common.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_holder_service.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_holder_service_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/block_store.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_store.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/session_manager.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/master_connector.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/replicator.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/ytree_integration.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_holder_service_rpc.proto YT_SOURCE_FILES )

