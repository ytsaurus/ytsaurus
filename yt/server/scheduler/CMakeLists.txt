#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/scheduler )
set( SRCS
  ${BASE}/chunk_list_pool.cpp
  ${BASE}/chunk_pool.cpp
  ${BASE}/exec_node.cpp
  ${BASE}/fifo_strategy.cpp
  ${BASE}/job.cpp
  ${BASE}/job_resources.cpp
  ${BASE}/map_controller.cpp
  ${BASE}/master_connector.cpp
  ${BASE}/merge_controller.cpp
  ${BASE}/null_strategy.cpp
  ${BASE}/operation.cpp
  ${BASE}/operation_controller_detail.cpp
  ${BASE}/private.cpp
  ${BASE}/progress_counter.cpp
  ${BASE}/samples_fetcher.cpp
  ${BASE}/scheduler.cpp
  ${BASE}/sort_controller.cpp
  ${BASE}/helpers.cpp
)
set( HDRS
  ${BASE}/chunk_list_pool.h
  ${BASE}/chunk_pool.h
  ${BASE}/config.h
  ${BASE}/exec_node.h
  ${BASE}/fifo_strategy.h
  ${BASE}/job.h
  ${BASE}/job_resources.h
  ${BASE}/map_controller.h
  ${BASE}/master_connector.h
  ${BASE}/merge_controller.h
  ${BASE}/null_strategy.h
  ${BASE}/operation.h
  ${BASE}/operation_controller.h
  ${BASE}/operation_controller_detail.h
  ${BASE}/private.h
  ${BASE}/progress_counter.h
  ${BASE}/public.h
  ${BASE}/samples_fetcher.h
  ${BASE}/scheduler.h
  ${BASE}/scheduler_strategy.h
  ${BASE}/sort_controller.h
  ${BASE}/helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
