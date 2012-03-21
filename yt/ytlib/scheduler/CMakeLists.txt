#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/scheduler )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/redirector_service.cpp
  ${BASE}/operation.cpp
  ${BASE}/job.cpp
  ${BASE}/exec_node.cpp
  ${BASE}/scheduler.cpp
  ${BASE}/fifo_strategy.cpp
  ${BASE}/map_controller.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/config.h
  ${BASE}/redirector_service.h
  ${BASE}/scheduler_service_proxy.h
  ${BASE}/operation.h
  ${BASE}/job.h
  ${BASE}/exec_node.h
  ${BASE}/scheduler.h
  ${BASE}/scheduler_strategy.h
  ${BASE}/fifo_strategy.h
  ${BASE}/operation_controller.h
  ${BASE}/map_controller.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/jobs.proto YT_SOURCE_FILES )
protoc( ${BASE}/scheduler_service.proto YT_SOURCE_FILES )
