#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/tablet_server )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/tablet_cell.cpp
  ${BASE}/tablet.cpp
  ${BASE}/tablet_tracker.cpp
  ${BASE}/tablet_manager.cpp
  ${BASE}/tablet_cell_proxy.cpp
  ${BASE}/tablet_proxy.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/tablet_cell.h
  ${BASE}/tablet.h
  ${BASE}/tablet_tracker.h
  ${BASE}/tablet_manager.h
  ${BASE}/tablet_cell_proxy.h
  ${BASE}/tablet_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_manager.proto YT_SOURCE_FILES )
