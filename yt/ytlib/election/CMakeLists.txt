#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cell_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/election_service_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/config.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/cell_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/election_service_mock.h
  ${CMAKE_CURRENT_LIST_DIR}/election_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/election_service.proto YT_SOURCE_FILES )
