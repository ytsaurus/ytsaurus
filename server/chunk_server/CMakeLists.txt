#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/chunk_server )
set( SRCS
  ${BASE}/chunk.cpp
  ${BASE}/chunk_list.cpp
  ${BASE}/chunk_list_proxy.cpp
  ${BASE}/chunk_manager.cpp
  ${BASE}/chunk_owner_base.cpp
  ${BASE}/chunk_owner_node_proxy.cpp
  ${BASE}/chunk_placement.cpp
  ${BASE}/chunk_proxy.cpp
  ${BASE}/chunk_replica.cpp
  ${BASE}/chunk_replicator.cpp
  ${BASE}/chunk_sealer.cpp
  ${BASE}/chunk_service.cpp
  ${BASE}/chunk_tree.cpp
  ${BASE}/chunk_tree_balancer.cpp
  ${BASE}/chunk_tree_statistics.cpp
  ${BASE}/chunk_tree_traversing.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/helpers.cpp
  ${BASE}/job.cpp
  ${BASE}/job_tracker_service.cpp
  ${BASE}/private.cpp
)

set( HDRS
  ${BASE}/chunk.h
  ${BASE}/chunk-inl.h
  ${BASE}/chunk_list.h
  ${BASE}/chunk_list-inl.h
  ${BASE}/chunk_list_proxy.h
  ${BASE}/chunk_manager.h
  ${BASE}/chunk_owner_base.h
  ${BASE}/chunk_owner_node_proxy.h
  ${BASE}/chunk_owner_type_handler-inl.h
  ${BASE}/chunk_owner_type_handler.h
  ${BASE}/chunk_placement.h
  ${BASE}/chunk_proxy.h
  ${BASE}/chunk_replica-inl.h
  ${BASE}/chunk_replica.h
  ${BASE}/chunk_replicator.h
  ${BASE}/chunk_sealer.h
  ${BASE}/chunk_service.h
  ${BASE}/chunk_tree.h
  ${BASE}/chunk_tree_balancer.h
  ${BASE}/chunk_tree_statistics.h
  ${BASE}/chunk_tree_traversing.h
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/helpers-inl.h
  ${BASE}/helpers.h
  ${BASE}/job.h
  ${BASE}/job_tracker_service.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_manager.proto YT_SOURCE_FILES )
