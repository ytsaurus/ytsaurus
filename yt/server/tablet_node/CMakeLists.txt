#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/tablet_node )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/tablet_slot.cpp
  ${BASE}/tablet_cell_controller.cpp
  ${BASE}/slot_automaton.cpp
  ${BASE}/tablet_manager.cpp
  ${BASE}/tablet.cpp
  ${BASE}/serialize.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/config.h
  ${BASE}/tablet_slot.h
  ${BASE}/tablet_cell_controller.h
  ${BASE}/slot_automaton.h
  ${BASE}/tablet_manager.h
  ${BASE}/tablet.h
  ${BASE}/serialize.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_manager.proto YT_SOURCE_FILES )
