#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder )
set( SRCS
  ${BASE}/block_store.cpp
  ${BASE}/chunk_holder_service.cpp
  ${BASE}/chunk_store.cpp
  ${BASE}/common.cpp
  ${BASE}/master_connector.cpp
  ${BASE}/replicator.cpp
  ${BASE}/session_manager.cpp
  ${BASE}/ytree_integration.cpp
)
set( HDRS
  ${BASE}/block_store.h
  ${BASE}/chunk_holder_service.h
  ${BASE}/chunk_holder_service_rpc.h
  ${BASE}/chunk_store.h
  ${BASE}/common.h
  ${BASE}/master_connector.h
  ${BASE}/replicator.h
  ${BASE}/session_manager.h
  ${BASE}/ytree_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_holder_service_rpc.proto YT_SOURCE_FILES )
