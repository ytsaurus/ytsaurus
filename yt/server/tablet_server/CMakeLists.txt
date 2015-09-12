#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_cell.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_cell_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_tracker.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_cell.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_cell_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_tracker.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/tablet_manager.proto YT_SOURCE_FILES )
