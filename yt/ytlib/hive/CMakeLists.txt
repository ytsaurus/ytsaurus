#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cell_directory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cluster_directory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/cell_directory.h
  ${CMAKE_CURRENT_LIST_DIR}/cluster_directory.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_supervisor_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/hive_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/transaction_supervisor_service.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/hive_service.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/cell_directory.proto YT_SOURCE_FILES )
