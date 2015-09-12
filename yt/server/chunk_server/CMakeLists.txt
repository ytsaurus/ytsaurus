#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/chunk.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_list.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_list_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_node_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_placement.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_replica.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_replicator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_sealer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_balancer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_statistics.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_traversing.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/job.cpp
  ${CMAKE_CURRENT_LIST_DIR}/job_tracker_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/chunk.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_list.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_list_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_base.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_node_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_type_handler-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_owner_type_handler.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_placement.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_replica-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_replica.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_replicator.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_sealer.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_service.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_balancer.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_statistics.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_traversing.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/job.h
  ${CMAKE_CURRENT_LIST_DIR}/job_tracker_service.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_manager.proto YT_SOURCE_FILES )
