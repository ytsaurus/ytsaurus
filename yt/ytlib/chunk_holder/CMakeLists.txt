SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/common.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_holder.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_holder_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/block_store.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_store.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/session.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/master_connector.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/replicator.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder/chunk_holder.proto YT_SOURCE_FILES )
