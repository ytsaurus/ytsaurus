#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_holder )
set( SRCS
  ${BASE}/block_store.cpp
  ${BASE}/chunk.cpp
  ${BASE}/chunk_cache.cpp
  ${BASE}/chunk_holder_service.cpp
  ${BASE}/chunk_store.cpp
  ${BASE}/chunk_registry.cpp
  ${BASE}/common.cpp
  ${BASE}/job_executor.cpp
  ${BASE}/location.cpp
  ${BASE}/master_connector.cpp
  ${BASE}/reader_cache.cpp
  ${BASE}/session_manager.cpp
  ${BASE}/ytree_integration.cpp
)
set( HDRS
  ${BASE}/block_store.h
  ${BASE}/chunk.h
  ${BASE}/chunk_cache.h
  ${BASE}/chunk_holder_service.h
  ${BASE}/chunk_holder_service_proxy.h
  ${BASE}/chunk_store.h
  ${BASE}/chunk_registry.h
  ${BASE}/common.h
  ${BASE}/job_executor.h
  ${BASE}/location.h
  ${BASE}/master_connector.h
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
