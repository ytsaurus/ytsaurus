#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/tablet_server )
set( SRCS
  ${BASE}/cypress_integration.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/tablet.cpp
  ${BASE}/tablet_cell.cpp
  ${BASE}/tablet_cell_proxy.cpp
  ${BASE}/tablet_manager.cpp
  ${BASE}/tablet_proxy.cpp
  ${BASE}/tablet_tracker.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/tablet.h
  ${BASE}/tablet_cell.h
  ${BASE}/tablet_cell_proxy.h
  ${BASE}/tablet_manager.h
  ${BASE}/tablet_proxy.h
  ${BASE}/tablet_tracker.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_manager.proto YT_SOURCE_FILES )
