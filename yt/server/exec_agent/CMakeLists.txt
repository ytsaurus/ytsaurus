#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/exec_agent )
set( SRCS
  ${BASE}/environment_manager.cpp
  ${BASE}/job.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/scheduler_connector.cpp
  ${BASE}/slot.cpp
  ${BASE}/slot_manager.cpp
  ${BASE}/supervisor_service.cpp
  ${BASE}/unsafe_environment.cpp
)

set( HDRS
  ${BASE}/config.h
  ${BASE}/environment.h
  ${BASE}/environment_manager.h
  ${BASE}/job.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/scheduler_connector.h
  ${BASE}/slot.h
  ${BASE}/slot_manager.h
  ${BASE}/supervisor_service.h
  ${BASE}/supervisor_service_proxy.h
  ${BASE}/unsafe_environment.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/supervisor_service.proto YT_SOURCE_FILES )
