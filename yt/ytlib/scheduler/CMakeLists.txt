#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/scheduler )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/scheduler_channel.cpp
  ${BASE}/operation.cpp
  ${BASE}/job.cpp
  ${BASE}/exec_node.cpp
  ${BASE}/scheduler.cpp
  ${BASE}/null_strategy.cpp
  ${BASE}/fifo_strategy.cpp
  ${BASE}/operation_controller_detail.cpp
  ${BASE}/map_controller.cpp
  ${BASE}/merge_controller.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/config.h
  ${BASE}/scheduler_channel.h
  ${BASE}/scheduler_proxy.h
  ${BASE}/operation.h
  ${BASE}/job.h
  ${BASE}/exec_node.h
  ${BASE}/scheduler.h
  ${BASE}/scheduler_strategy.h
  ${BASE}/null_strategy.h
  ${BASE}/fifo_strategy.h
  ${BASE}/operation_controller.h
  ${BASE}/operation_controller_detail.h
  ${BASE}/map_controller.h
  ${BASE}/merge_controller.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/jobs.proto YT_SOURCE_FILES )
protoc( ${BASE}/scheduler_service.proto YT_SOURCE_FILES )
