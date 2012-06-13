#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder )
set( SRCS
  ${BASE}/block_store.cpp
  ${BASE}/bootstrap.cpp
  ${BASE}/chunk.cpp
  ${BASE}/chunk_cache.cpp
  ${BASE}/chunk_holder_service.cpp
  ${BASE}/chunk_meta_extensions.cpp
  ${BASE}/chunk_registry.cpp
  ${BASE}/chunk_store.cpp
  ${BASE}/job_executor.cpp
  ${BASE}/location.cpp
  ${BASE}/master_connector.cpp
  ${BASE}/peer_block_table.cpp
  ${BASE}/peer_block_updater.cpp
  ${BASE}/private.cpp
  ${BASE}/reader_cache.cpp
  ${BASE}/session_manager.cpp
  ${BASE}/ytree_integration.cpp
)
set( HDRS
  ${BASE}/block_store.h
  ${BASE}/bootstrap.h
  ${BASE}/chunk.h
  ${BASE}/chunk_cache.h
  ${BASE}/chunk_holder_service.h
  ${BASE}/chunk_holder_service_proxy.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_registry.h
  ${BASE}/chunk_store.h
  ${BASE}/config.h
  ${BASE}/job_executor.h
  ${BASE}/location.h
  ${BASE}/master_connector.h
  ${BASE}/peer_block_table.h
  ${BASE}/peer_block_updater.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/reader_cache.h
  ${BASE}/session_manager.h
  ${BASE}/ytree_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_holder_service.proto YT_SOURCE_FILES )
