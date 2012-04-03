#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/exec_agent )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/bootstrap.cpp
  ${BASE}/environment_manager.cpp
  ${BASE}/job.cpp
  ${BASE}/job_manager.cpp
  ${BASE}/slot.cpp
  ${BASE}/supervisor_service.cpp
  ${BASE}/unsafe_environment.cpp
  ${BASE}/scheduler_connector.cpp
)

set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/environment.h
  ${BASE}/environment_manager.h
  ${BASE}/job.h
  ${BASE}/job_manager.h
  ${BASE}/slot.h
  ${BASE}/supervisor_service.h
  ${BASE}/supervisor_service_proxy.h
  ${BASE}/unsafe_environment.h
  ${BASE}/scheduler_connector.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/supervisor_service.proto YT_SOURCE_FILES )
