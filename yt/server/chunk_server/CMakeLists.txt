#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/chunk_server )
set( SRCS
  ${BASE}/chunk.cpp
  ${BASE}/chunk_proxy.cpp
  ${BASE}/chunk_list.cpp
  ${BASE}/chunk_list_proxy.cpp
  ${BASE}/chunk_manager.cpp
  ${BASE}/chunk_placement.cpp
  ${BASE}/chunk_replicator.cpp
  ${BASE}/chunk_service.cpp
  ${BASE}/chunk_tree_balancer.cpp
  ${BASE}/chunk_tree_ref.cpp
  ${BASE}/chunk_tree_statistics.cpp
  ${BASE}/chunk_tree_traversing.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/helpers.cpp
  ${BASE}/job.cpp
  ${BASE}/job_list.cpp
  ${BASE}/node.cpp
  ${BASE}/node_authority.cpp
  ${BASE}/node_lease_tracker.cpp
  ${BASE}/node_statistics.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/chunk.h
  ${BASE}/chunk_proxy.h
  ${BASE}/chunk_list.h
  ${BASE}/chunk_list_proxy.h
  ${BASE}/chunk_manager.h
  ${BASE}/chunk_placement.h
  ${BASE}/chunk_replicator.h
  ${BASE}/chunk_service.h
  ${BASE}/chunk_service_proxy.h
  ${BASE}/chunk_tree_balancer.h
  ${BASE}/chunk_tree_ref.h
  ${BASE}/chunk_tree_statistics.h
  ${BASE}/chunk_tree_traversing.h
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/helpers.h
  ${BASE}/job.h
  ${BASE}/job_list.h
  ${BASE}/node.h
  ${BASE}/node_authority.h
  ${BASE}/node_lease_tracker.h
  ${BASE}/node_statistics.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_service.proto YT_SOURCE_FILES )

include_directories(
  ${CMAKE_BINARY_DIR}/yt/ytlib/table_client
)
