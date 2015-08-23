#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/data_node )
set( SRCS
  ${BASE}/artifact.cpp
  ${BASE}/blob_chunk.cpp
  ${BASE}/blob_reader_cache.cpp
  ${BASE}/blob_session.cpp
  ${BASE}/block_cache.cpp
  ${BASE}/block_store.cpp
  ${BASE}/chunk.cpp
  ${BASE}/chunk_cache.cpp
  ${BASE}/chunk_detail.cpp
  ${BASE}/chunk_registry.cpp
  ${BASE}/chunk_store.cpp
  ${BASE}/data_node_service.cpp
  ${BASE}/disk_health_checker.cpp
  ${BASE}/job.cpp
  ${BASE}/journal_chunk.cpp
  ${BASE}/journal_dispatcher.cpp
  ${BASE}/journal_manager.cpp
  ${BASE}/journal_session.cpp
  ${BASE}/local_chunk_reader.cpp
  ${BASE}/location.cpp
  ${BASE}/master_connector.cpp
  ${BASE}/peer_block_table.cpp
  ${BASE}/peer_block_updater.cpp
  ${BASE}/private.cpp
  ${BASE}/session_detail.cpp
  ${BASE}/session_manager.cpp
  ${BASE}/ytree_integration.cpp
)
set( HDRS
  ${BASE}/artifact.h
  ${BASE}/blob_chunk.h
  ${BASE}/blob_reader_cache.h
  ${BASE}/blob_session.h
  ${BASE}/block_cache.h
  ${BASE}/block_store.h
  ${BASE}/chunk.h
  ${BASE}/chunk_cache.h
  ${BASE}/chunk_detail.h
  ${BASE}/chunk_registry.h
  ${BASE}/chunk_store.h
  ${BASE}/config.h
  ${BASE}/data_node_service.h
  ${BASE}/disk_health_checker.h
  ${BASE}/job.h
  ${BASE}/journal_chunk.h
  ${BASE}/journal_dispatcher.h
  ${BASE}/journal_manager.h
  ${BASE}/journal_session.h
  ${BASE}/local_chunk_reader.h
  ${BASE}/location.h
  ${BASE}/master_connector.h
  ${BASE}/peer_block_table.h
  ${BASE}/peer_block_updater.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/session.h
  ${BASE}/session_detail.h
  ${BASE}/session_manager.h
  ${BASE}/ytree_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/artifact.proto YT_SOURCE_FILES )
