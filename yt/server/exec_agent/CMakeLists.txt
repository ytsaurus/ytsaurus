#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/environment_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/job.cpp
  ${CMAKE_CURRENT_LIST_DIR}/job_prober_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_connector.cpp
  ${CMAKE_CURRENT_LIST_DIR}/slot.cpp
  ${CMAKE_CURRENT_LIST_DIR}/slot_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/supervisor_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/unsafe_environment.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/environment.h
  ${CMAKE_CURRENT_LIST_DIR}/environment_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/job.h
  ${CMAKE_CURRENT_LIST_DIR}/job_prober_service.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_connector.h
  ${CMAKE_CURRENT_LIST_DIR}/slot.h
  ${CMAKE_CURRENT_LIST_DIR}/slot_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/supervisor_service.h
  ${CMAKE_CURRENT_LIST_DIR}/supervisor_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/unsafe_environment.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/supervisor_service.proto YT_SOURCE_FILES )
