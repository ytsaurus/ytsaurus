#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/data_node )
set( SRCS
  ${BASE}/block_store.cpp
  ${BASE}/chunk.cpp
  ${BASE}/blob_chunk.cpp
  ${BASE}/journal_chunk.cpp
  ${BASE}/chunk_cache.cpp
  ${BASE}/chunk_registry.cpp
  ${BASE}/chunk_store.cpp
  ${BASE}/data_node_service.cpp
  ${BASE}/disk_health_checker.cpp
  ${BASE}/job.cpp
  ${BASE}/location.cpp
  ${BASE}/master_connector.cpp
  ${BASE}/peer_block_table.cpp
  ${BASE}/peer_block_updater.cpp
  ${BASE}/private.cpp
  ${BASE}/reader_cache.cpp
  ${BASE}/session_manager.cpp
  ${BASE}/ytree_integration.cpp
  ${BASE}/local_chunk_reader.cpp
)
set( HDRS
  ${BASE}/block_store.h
  ${BASE}/chunk.h
  ${BASE}/blob_chunk.h
  ${BASE}/journal_chunk.h
  ${BASE}/chunk_cache.h
  ${BASE}/chunk_registry.h
  ${BASE}/chunk_store.h
  ${BASE}/config.h
  ${BASE}/data_node_service.h
  ${BASE}/disk_health_checker.h
  ${BASE}/job.h
  ${BASE}/location.h
  ${BASE}/master_connector.h
  ${BASE}/peer_block_table.h
  ${BASE}/peer_block_updater.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/reader_cache.h
  ${BASE}/session_manager.h
  ${BASE}/ytree_integration.h
  ${BASE}/local_chunk_reader.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
